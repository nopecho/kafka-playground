package com.nopecho.kafka.support

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.ContainerProperties.AckMode
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.MessageListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.ContainerTestUtils
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/**
 * EmbeddedKafka 를 사용하여 Kafka 테스트를 위한 기본 설정을 제공합니다.
 */
@EmbeddedKafka(
    partitions = 10,
)
abstract class EmbeddedKafkaSupport {

    // default kafka producer config
    protected val defaultProducerConfig: Map<String, Any> = mapOf(
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.RETRIES_CONFIG to "3",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer",
    )

    // default kafka consumer config
    protected val defaultConsumerConfig: Map<String, Any> = mapOf(
        ConsumerConfig.GROUP_ID_CONFIG to "test-group",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringDeserializer",
    )

    /**
     * @SpringBootTest
     * @EmbeddedKafka
     * 두 가지 애노테이션을 사용하면 테스트 환경에서 EmbeddedKafkaBroker 를 주입 받을 수 있습니다.
     * - @SpringBootTest 애노테이션은 EmbeddedKafkaSupport 클래스를 사용하는 테스트 클래스에 적용되어야 합니다.
     *
     * ref. https://docs.spring.io/spring-kafka/reference/testing.html#embedded-kafka-annotation
     */
    @Autowired
    protected lateinit var embeddedKafka: EmbeddedKafkaBroker

    /**
     * KafkaTemplate을 생성하는 메서드입니다.
     * 기본적으로 제공되는 EmbeddedKafka 기반의 프로듀서 설정을 사용합니다.
     *
     * @param config 추가적인 프로듀서 설정을 전달할 수 있는 매개변수입니다. defaultProducerConfig 를 override 합니다.
     */
    protected fun makeKafkaProducer(config: MutableMap<String, Any> = mutableMapOf()): KafkaTemplate<String, String> {
        val producerConfig = defaultProducerConfig.merge(config)
        val factory = makeProducerFactory(producerConfig)
        return KafkaTemplate(factory)
    }

    /**
     * KafkaConsumer을 생성하는 메서드입니다.
     * 기본적으로 제공되는 EmbeddedKafka 기반의 컨슈머 설정을 사용합니다.
     *
     * @param config 추가적인 컨슈머 설정을 전달할 수 있는 매개변수입니다. defaultConsumerConfig 를 override 합니다.
     */
    protected fun makeKafkaConsumer(config: MutableMap<String, Any> = mutableMapOf()): KafkaConsumer<String, String> {
        val consumerConfig = defaultConsumerConfig.merge(config)
        return KafkaConsumer(consumerConfig)
    }

    /**
     * KafkaListenerContainer를 생성하는 메서드입니다.
     * 기본적으로 제공되는 EmbeddedKafka 기반의 컨슈머 설정을 사용합니다.
     *
     * @param topics 구독할 토픽을 지정합니다.
     * @param ack ackMode 를 지정합니다. 기본값은 AckMode.RECORD 입니다.
     * @param config 추가적인 컨슈머 설정을 전달할 수 있는 매개변수입니다. defaultConsumerConfig 를 override 합니다.
     */
    protected fun listenKafka(
        topics: Array<out String> = emptyArray(),
        ack: AckMode = AckMode.RECORD,
        config: Map<String, Any> = mapOf(),
    ): KafkaListenerMailBox {
        val consumerConfig = defaultConsumerConfig.merge(config)
        val consumerFactory = makeConsumerFactory(consumerConfig)

        val (containerConfig, mailbox) = makeContainerConfig(topics, ack)
        thread(name = "embedded-kafka-listener") {
            KafkaMessageListenerContainer(consumerFactory, containerConfig).apply {
                start()
                ContainerTestUtils.waitForAssignment(this, embeddedKafka.partitionsPerTopic)
            }
        }
        return mailbox
    }

    /**
     * KafkaConsumer을 사용하여 특정 토픽의 모든 파티션에 대한 커밋된 오프셋을 조회하는 메서드입니다.
     *
     * @param topic 조회할 토픽의 이름입니다.
     */
    protected fun KafkaConsumer<String, String>.getAllCommittedOffsets(topic: String): Map<TopicPartition, Long> {
        // 토픽의 모든 파티션 정보 가져오기
        val partitionInfos = this.partitionsFor(topic)

        // 각 파티션에 대한 TopicPartition 객체 생성
        val topicPartitions = partitionInfos.map { TopicPartition(topic, it.partition()) }.toSet()

        // 모든 파티션의 커밋된 오프셋 조회
        return this.committed(topicPartitions).mapValues { it.value?.offset() ?: 0 }
    }

    private fun makeProducerFactory(config: MutableMap<String, Any>): ProducerFactory<String, String> {
        return DefaultKafkaProducerFactory(config)
    }

    private fun makeConsumerFactory(config: MutableMap<String, Any>): ConsumerFactory<String, String> {
        return DefaultKafkaConsumerFactory(config)
    }

    private fun Map<String, Any>.merge(config: Map<String, Any>): MutableMap<String, Any> {
        return this.toMutableMap().apply {
            this.putAll(config)
            this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = embeddedKafka.brokersAsString
        }
    }

    /**
     * KafkaListenerContainer 설정을 위한 ContainerProperties를 생성하는 메서드입니다.
     */
    private fun makeContainerConfig(
        topics: Array<out String>,
        ack: AckMode
    ): Pair<ContainerProperties, KafkaListenerMailBox> {
        val mailbox = KafkaListenerMailBox()
        val properties = ContainerProperties(*topics).apply {
            ackMode = ack
            messageListener = if (ack == AckMode.MANUAL || ack == AckMode.MANUAL_IMMEDIATE) {
                AcknowledgingMessageListener<String, String> { data, acknowledgment ->
                    mailbox.offer(data, acknowledgment)
                    println("✅ acknowledgment listen -> ${data.key()}: ${data.value()}. size: ${mailbox.size()}")
                }
            } else {
                MessageListener<String, String> { data ->
                    mailbox.offer(data)
                    println("✅ listen -> ${data.key()}: ${data.value()}. size: ${mailbox.size()}")
                }
            }
        }
        return properties to mailbox
    }


    /**
     * KafkaListenerMailBox 클래스는 KafkaListenerContainer에서 수신한 메시지를 저장하는 BlockingQueue를 포함합니다.
     *
     * @property queue BlockingQueue<ConsumerRecordWithAck> KafkaListenerContainer에서 수신한 메시지를 저장하는 큐입니다.
     */
    data class KafkaListenerMailBox(
        val queue: BlockingQueue<ConsumerRecordWithAck> = LinkedBlockingQueue(),
    ) {
        /**
         * BlockingQueue 의 offer() 메서드를 사용하여 큐에 요소를 추가합니다.
         * 이 메서드는 큐가 가득 차면 대기하지 않고 false를 반환합니다.
         *
         * @param record 큐에 추가할 요소입니다.
         * @return 큐에 요소를 성공적으로 추가하면 true, 그렇지 않으면 false를 반환합니다.
         */
        fun offer(record: ConsumerRecordWithAck): Boolean {
            return queue.offer(record)
        }

        /**
         * BlockingQueue 의 offer() 메서드를 사용하여 큐에 요소를 추가합니다.
         *
         * @param record 큐에 추가할 요소입니다.
         * @param ack Acknowledgment 객체입니다. 수동 확인 모드에서 사용됩니다.
         */
        fun offer(record: ConsumerRecord<String, String>, ack: Acknowledgment? = null): Boolean {
            return queue.offer(ConsumerRecordWithAck(record, ack))
        }

        /**
         * BlockingQueue 의 poll() 메서드를 사용하여 큐에서 요소를 가져옵니다.
         * 이 메서드는 지정된 시간 동안 대기하며, 큐에 요소가 없으면 null을 반환합니다.
         *
         * @param timeout 대기할 시간(밀리초)입니다. 기본값은 1000ms 입니다.
         */
        fun poll(timeout: Long = 1000L): ConsumerRecordWithAck? {
            return queue.poll(timeout, TimeUnit.MILLISECONDS)
        }

        /**
         * BlockingQueue 의 size() 메서드를 사용하여 현재 큐에 있는 요소의 개수를 반환합니다.
         * @return 현재 큐에 있는 요소의 개수
         */
        fun size(): Int {
            return queue.size
        }

        /**
         * 큐에 있는 모든 메시지를 확인하고, 각 메시지에 대해 ack() 메서드를 호출하여 수동으로 확인합니다.
         * 이 메서드는 수동 확인 모드에서만 사용해야 합니다.
         */
        fun ackAll() {
            queue.forEach {
                it.ack?.acknowledge()
            }
        }
    }

    data class ConsumerRecordWithAck(
        val record: ConsumerRecord<String, String>,
        val ack: Acknowledgment? = null
    )
}