package com.nopecho.kafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class StreamsApplication

fun main(args: Array<String>) {
    runApplication<StreamsApplication>(*args)
}