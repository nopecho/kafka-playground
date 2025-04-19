package com.nopecho.kafka

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.util.StopWatch
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = ["test"],
    brokerProperties = ["listeners=PLAINTEXT://localhost:9099"],
    ports = [9099],
)
class VirtualThreadTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Test
    fun `thread pool executor`() {
        val client = HeavyTaskClient()
        val executor = Executors.newFixedThreadPool(10)

        stopwatch {
            val map = (1..100).map {
                CompletableFuture.supplyAsync({
                    client.call(
                        topic = "test",
                        message = "CompletableFuture $it",
                        template = kafkaTemplate
                    )
                }, executor)
                    .thenApply { println("Result: $it") }
                    .exceptionally { println("Error: ${it.message}") }
            }
            CompletableFuture.allOf(*map.toTypedArray()).join()
        }
    }

    @Test
    fun `thread pool coroutine`() = runTest {
        val client = HeavyTaskClient()
        val executor = Executors.newFixedThreadPool(10)

        stopwatch {
            runBlocking(executor.asCoroutineDispatcher()) {
                val deferredList = (1..100).map {
                    async {
                        client.call(
                            topic = "test",
                            message = "ThreadPool coroutine $it",
                            template = kafkaTemplate
                        )
                    }
                }
                deferredList.awaitAll()
            }
        }
    }

    @Test
    fun `virtual thread coroutine`() {
        val client = HeavyTaskClient()
        val executor = Executors.newVirtualThreadPerTaskExecutor()

        stopwatch {
            runBlocking(executor.asCoroutineDispatcher()) {
                val deferredList = (1..100_000).map {
                    async {
                        client.call(
                            topic = "test",
                            message = "VirtualThread coroutine $it",
                            template = kafkaTemplate
                        )
                    }
                }
                deferredList.awaitAll()
            }
        }
    }

    private fun stopwatch(block: () -> Unit) {
        val stopwatch = StopWatch()
        stopwatch.start()
        block()
        stopwatch.stop()
        stopwatch.prettyPrint(TimeUnit.SECONDS)
    }

    inner class HeavyTaskClient {
        fun call(topic: String, message: String, template: KafkaTemplate<String, String>): String {
            Thread.sleep((1000..3000).random().toLong())
            template.send(topic, message)
                .thenApplyAsync({
                    println("Message sent successfully: $it")
                }, Executors.newVirtualThreadPerTaskExecutor())
                .exceptionally {
                    println("Failed to send message: ${it.message}")
                }
            return message
        }
    }
}