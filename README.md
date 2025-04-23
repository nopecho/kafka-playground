# Kafka Playground

테스트 중심으로 Kotlin + Spring Boot 환경에서 Apache Kafka를 사용하는 다양한 예제를 탐구합니다.

## kafka-test

`kafka-test` 모듈은 Spring의 내장 Kafka 브로커를 사용하여 Kafka 애플리케이션을 테스트하기 위한 유틸리티와 지원 클래스를 제공합니다.

- **내장 Kafka 테스트**: Spring의 내장 Kafka 브로커를 사용하여 외부 의존성 없이 Kafka 프로듀서와 컨슈머를 테스트
- **테스트 컨테이너 지원**: Docker 컨테이너에서 Kafka를 테스트하기 위한 인프라 (진행 중)
- **유틸리티 클래스**: 프로듀서, 컨슈머 및 리스너를 생성하기 위한 헬퍼 메서드
- **메시지 추적**: Kafka 토픽으로 전송된 메시지를 캡처하고 검증

### Dependency

```gradle
dependencies {
    implementation("org.springframework.kafka:spring-kafka")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.testcontainers:kafka")
}
```

### EmbeddedKafka 를 사용한 테스트

`EmbeddedKafkaSupport` 클래스는 Spring의 내장 Kafka 브로커를 사용한 테스트의 기반을 제공합니다:

```kotlin
@SpringBootTest
class MyKafkaTest : EmbeddedKafkaSupport() {

    @Test
    fun testKafkaMessaging() {
        // 테스트 토픽에 대한 리스너 생성
        val mailBox = listenKafka(
            topics = arrayOf("test-topic"),
            ack = AckMode.MANUAL,
            config = mapOf(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false")
        )

        // 프로듀서 생성 및 메시지 전송
        val producer = makeKafkaProducer()
        repeat(10) {
            producer.send("test-topic", "test-message-$it").join()
        }

        // 수신된 메시지 확인
        for (record in mailBox.queue) {
            println("수신된 메시지: offset=${record.record.offset()}, value=${record.record.value()}")
        }

        // 커밋된 오프셋 확인
        val consumer = makeKafkaConsumer()
        val offsets = consumer.getAllCommittedOffsets("test-topic")
        for ((partition, offset) in offsets) {
            println("커밋된 오프셋: ${partition.topic()}-${partition.partition()} offset: ${offset}")
        }
    }
}
```

### EmbeddedKafkaSupport

다음을 제공하는 추상 클래스:

- Kafka 프로듀서 및 컨슈머에 대한 기본 구성
- Kafka 프로듀서 및 컨슈머 생성을 위한 메서드
- 토픽에서 메시지를 캡처하기 위한 리스너 설정
- 커밋된 오프셋을 확인하기 위한 유틸리티

`EmbeddedKafkaSupport` 클래스는 Spring의 내장 Kafka 브로커를 사용하여 Kafka 애플리케이션을 테스트하기 위한 기반 클래스입니다.

이 클래스는 테스트 환경에서 Kafka 프로듀서와
컨슈머를 쉽게 생성하고 관리할 수 있는 다양한 유틸리티 메서드를 제공합니다.

**주요 특징:**

1. **내장 Kafka 브로커 자동 설정**: `@EmbeddedKafka` 애노테이션을 통해 테스트 환경에서 사용할 내장 Kafka 브로커를 자동으로 설정합니다.
2. **기본 설정 제공**: Kafka 프로듀서와 컨슈머에 대한 기본 설정을 제공하여 테스트 코드를 간결하게 유지할 수 있습니다.
3. **유연한 구성**: 기본 설정을 오버라이드하여 테스트 요구사항에 맞게 Kafka 구성을 조정할 수 있습니다.
4. **메시지 캡처 및 검증**: 토픽으로 전송된 메시지를 캡처하고 검증하기 위한 도구를 제공합니다.

**주요 메서드:**

1. **makeKafkaProducer**: Kafka 프로듀서를 생성하는 메서드입니다.
   ```kotlin
   fun makeKafkaProducer(config: MutableMap<String, Any> = mutableMapOf()): KafkaTemplate<String, String>
   ```

2. **makeKafkaConsumer**: Kafka 컨슈머를 생성하는 메서드입니다.
   ```kotlin
   fun makeKafkaConsumer(config: MutableMap<String, Any> = mutableMapOf()): KafkaConsumer<String, String>
   ```

3. **listenKafka**: 지정된 토픽에 대한 리스너를 설정하고 메시지를 캡처하는 메서드입니다.
   ```kotlin
   fun listenKafka(
       topics: Array<out String> = emptyArray(),
       ack: AckMode = AckMode.RECORD,
       config: Map<String, Any> = mapOf(),
   ): KafkaListenerMailBox
   ```

4. **getAllCommittedOffsets**: 특정 토픽의 모든 파티션에 대한 커밋된 오프셋을 조회하는 확장 메서드입니다.
   ```kotlin
   fun KafkaConsumer<String, String>.getAllCommittedOffsets(topic: String): Map<TopicPartition, Long>
   ```

**사용 예제:**

1. **기본 사용법**:

```kotlin
@SpringBootTest
class MyKafkaTest : EmbeddedKafkaSupport() { // EmbeddedKafkaSupport() 상속

    @Test
    fun testKafkaMessaging() {
        // 리스너 설정
        val mailBox = listenKafka(
            topics = arrayOf("my-topic"),
            ack = AckMode.RECORD
        )

        // 프로듀서 생성 및 메시지 전송
        val producer = makeKafkaProducer()
        producer.send("my-topic", "Hello, Kafka!").join()

        // 메시지 확인
        val record = mailBox.queue.poll(5000, TimeUnit.MILLISECONDS)
        assertEquals("Hello, Kafka!", record?.record?.value())
    }
}
```

2. **커스텀 설정으로 프로듀서 생성**:

```kotlin
val producerConfig = mutableMapOf<String, Any>(
    ProducerConfig.ACKS_CONFIG to "1",
    ProducerConfig.RETRIES_CONFIG to "5"
)
val producer = makeKafkaProducer(producerConfig)
```

3. **수동 확인 모드 사용**:

```kotlin
// 수동 확인 모드로 리스너 설정
val mailBox = listenKafka(
    topics = arrayOf("test-topic"),
    ack = AckMode.MANUAL,
    config = mapOf(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false")
)

// 메시지 전송
val producer = makeKafkaProducer()
producer.send("test-topic", "test-message").join()

// 메시지 처리 후 수동으로 확인
mailBox.queue.forEach { record ->
    // 메시지 처리 로직
    println("메시지 처리: ${record.record.value()}")

    // 메시지 수동 확인
    record.ack?.acknowledge()
}
```

4. **커밋된 오프셋 확인**:

```kotlin
val consumer = makeKafkaConsumer()
val offsets = consumer.getAllCommittedOffsets("test-topic")
for ((partition, offset) in offsets) {
    println("파티션 ${partition.partition()}의 커밋된 오프셋: $offset")
}
```

##### KafkaListenerMailBox

다음과 같은 기능을 제공하는 클래스:

- 수신된 Kafka 메시지를 큐에 저장
- 메시지에 접근하고 확인하기 위한 메서드 제공
- 자동 및 수동 확인 모드 지원
