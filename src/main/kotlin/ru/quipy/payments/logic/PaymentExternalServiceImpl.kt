package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    // Параметры конфигурации
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // Лимитер по RPS
    private val rateLimiter: RateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    // Ограничение числа параллельных операций
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    // Пул потоков
    private val executor: ExecutorService = Executors.newFixedThreadPool(parallelRequests)

    // HTTP/2 клиент с привязкой к ExecutorService
    private val client: HttpClient = HttpClient.newBuilder()
        .executor(executor)
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(1))
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submitting payment: $paymentId, txId: $transactionId")

        // Фиксация момента отправки
        paymentESService.update(paymentId) {
            it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        executor.submit {
            // Проверка окна параллельных запросов
            while (true) {
                val result = ongoingWindow.putIntoWindow()
                if (result is NonBlockingOngoingWindow.WindowResponse.Success) break

                if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                    logger.warn("[$accountName] Timeout on entering window for $paymentId")
                    paymentESService.update(paymentId) {
                        it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                    }
                    return@submit
                }
            }

            // Проверка ограничения частоты
            while (!rateLimiter.tick()) {
                if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                    logger.warn("[$accountName] RPS deadline violation for $paymentId")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "RPS violation")
                    }
                    ongoingWindow.releaseWindow()
                    return@submit
                }
            }

            // Формируем URI
            val uri = URI.create(
                "http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            )

            // Собираем HTTP-запрос
            val request = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofMillis(1300))
                .header("x-idempotency-key", paymentId.toString())
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            // Отправляем запрос асинхронно
            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete { response, throwable ->
                    try {
                        if (throwable != null) {
                            // Ошибка сети, логируем
                            logger.error("[$accountName] Exception on payment $paymentId", throwable)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = throwable.message)
                            }
                        } else {
                            // Успешный или неуспешный HTTP-ответ
                            val body = try {
                                mapper.readValue(response.body(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.warn("[$accountName] Could not parse response for $paymentId", e)
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                            }

                            logger.info("[$accountName] Got response for $paymentId: success=${body.result}")

                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                        }
                    } finally {
                        // Всегда освобождаем слот
                        ongoingWindow.releaseWindow()
                    }
                }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
