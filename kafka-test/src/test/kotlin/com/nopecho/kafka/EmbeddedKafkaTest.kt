package com.nopecho.kafka

import com.nopecho.kafka.support.EmbeddedKafkaSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.listener.ContainerProperties.AckMode

@SpringBootTest
class EmbeddedKafkaTest : EmbeddedKafkaSupport() {

    @Test
    fun send() {
        val mailBox = listenKafka(
            topics = arrayOf("test-topic"),
            ack = AckMode.MANUAL,
            config = mapOf(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false")
        )

        val producer = makeKafkaProducer()
        repeat(10) {
            producer.send("test-topic", "value").join()
        }

        mailBox.queue.forEach {
            println("Received message:offset=${it.record.offset()} key=${it.record.key()}, value=${it.record.value()}, partition=${it.record.partition()}")
        }

        val consumer = makeKafkaConsumer()
        consumer.getAllCommittedOffsets("test-topic").forEach {
            println("offset: ${it.key.topic()}-${it.key.partition()} offset: ${it.value}")
        }
    }
}
