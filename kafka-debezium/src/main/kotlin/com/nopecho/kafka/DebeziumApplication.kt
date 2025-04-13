package com.nopecho.kafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class DebeziumApplication

fun main(args: Array<String>) {
    runApplication<DebeziumApplication>(*args)
}