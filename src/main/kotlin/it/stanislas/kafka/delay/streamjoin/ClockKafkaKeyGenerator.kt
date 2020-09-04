package it.stanislas.kafka.delay.streamjoin

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

object ClockKafkaKeyGenerator {
    private const val keyPattern = "yyyyMMddHHmmss"
    private val dateTimeFormatter =
        DateTimeFormatter.ofPattern(keyPattern).withZone(ZoneId.systemDefault())

    fun now(): String {
        return at(Instant.now())
    }

    fun at(time: Instant): String {
        return dateTimeFormatter.format(time)
    }
}