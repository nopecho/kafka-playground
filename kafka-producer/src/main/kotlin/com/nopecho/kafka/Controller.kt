package com.nopecho.kafka

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class Controller(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    @PostMapping("/send/{message}")
    fun sendMessage(@PathVariable message: String) {
        kafkaTemplate.send("sample", message)
            .whenComplete { record, ex ->
                when (ex) {
                    null -> println("Message sent successfully: $record")
                    else -> println("Failed to send message: $ex")
                }
            }
    }
}