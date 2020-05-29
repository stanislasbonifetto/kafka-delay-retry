package it.stanislas.kafka.retry;

import it.stanislas.kafka.ProducerA;
import it.stanislas.kafka.KafkaConfig;
import org.apache.kafka.streams.KafkaStreams;

public class Retry {
    public static void main(String[] args) {
        final KafkaConfig kafkaConfig = KafkaConfig.buildDefault();

        final KafkaStreams processorStream = ProcessorAtoB.createAndStart(kafkaConfig);

//        final ConsumerB consumerB = ConsumerB.createAndStart(kafkaConfig);

        final ProducerA producerA = ProducerA.createAndStart(kafkaConfig);
    }

}