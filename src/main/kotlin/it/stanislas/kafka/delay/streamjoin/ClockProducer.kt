package it.stanislas.kafka.delay.streamjoin

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.Disposable
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.TimeUnit
import java.util.stream.IntStream


class ClockProducer (
    private val kafkaProducer: KafkaProducer<String, String>,
    private val clockTopicName: String,
    private val clockKafkaKeyGenerator: ClockKafkaKeyGenerator
) {
    private val numberOfInstances = 2
    fun start() {
        IntStream.range(0, numberOfInstances)
            .forEach { sendTickEachSecond() }
    }

    private fun sendTickEachSecond(): Disposable {
        return Observable
            .interval(1, TimeUnit.SECONDS)
            .doOnNext { sendTick() }
            .subscribe()
    }

    private fun sendTick() {
        val key = clockKafkaKeyGenerator.now()
        val recordMetadataFeature =
            kafkaProducer.send(ProducerRecord<String, String>(clockTopicName, key, key))
        //        System.out.println("tick k:" + key + " v:" + value);
    }

    companion object {
        fun buildAndStart(
            bootstrapServers: String,
            clockTopicName: String,
            clockKafkaKeyGenerator: ClockKafkaKeyGenerator
        ): ClockProducer {
            val producer =
                build(bootstrapServers, clockTopicName, clockKafkaKeyGenerator)
            producer.start()
            return producer
        }

        fun build(
            bootstrapServers: String,
            clockTopicName: String,
            clockKafkaKeyGenerator: ClockKafkaKeyGenerator
        ): ClockProducer {
            return ClockProducer(
                ProducerFactory.buildProducer(bootstrapServers),
                clockTopicName,
                clockKafkaKeyGenerator
            )
        }
    }

}