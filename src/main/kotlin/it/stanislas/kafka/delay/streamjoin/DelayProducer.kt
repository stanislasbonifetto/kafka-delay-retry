package it.stanislas.kafka.delay.streamjoin

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Instant
import java.util.concurrent.Future


class DelayProducer constructor(
    private val kafkaProducer: KafkaProducer<String, String>,
    private val delayTopicName: String,
    private val clockKafkaKeyGenerator: ClockKafkaKeyGenerator
) {
    fun sendAt(message: String, fireAt: Instant): Future<RecordMetadata> {
        val fireTime = clockKafkaKeyGenerator.now()
        val key = clockKafkaKeyGenerator.at(fireAt)
        val recordMetadataFeature =
            kafkaProducer.send(ProducerRecord<String, String>(delayTopicName, key, message))
        println("send k:$key v:$message fireTime:$fireTime")
        return recordMetadataFeature
    }

    companion object {
        fun build(
            bootstrapServers: String,
            delayTopicName: String,
            clockKafkaKeyGenerator: ClockKafkaKeyGenerator
        ): DelayProducer {
            return DelayProducer(
                ProducerFactory.buildProducer(bootstrapServers),
                delayTopicName,
                clockKafkaKeyGenerator
            )
        }
    }

}