package it.stanislas.kafka.delay.streamjoin

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class ClockKafkaKeyGenerator {
    fun now(): String {
        return at(Instant.now())
    }

    fun at(time: Instant): String {
        return FORMATTER.format(time)
    }

    companion object {
        private const val KEY_PATTERN = "yyyyMMddHHmmss"
        private val FORMATTER =
            DateTimeFormatter.ofPattern(KEY_PATTERN).withZone(ZoneId.systemDefault())
    }
}