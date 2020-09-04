package it.stanislas.kafka.delay.streamjoin

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

object ProducerFactory {
    fun buildProducer(bootstrapServers: String): KafkaProducer<String, String> {
        val producerConfig = Properties()
        producerConfig[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        producerConfig[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        producerConfig[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        return KafkaProducer(producerConfig)
    }
}