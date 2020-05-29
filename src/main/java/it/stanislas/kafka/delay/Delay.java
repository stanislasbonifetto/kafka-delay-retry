package it.stanislas.kafka.delay;

import it.stanislas.kafka.ConsumerB;
import it.stanislas.kafka.KafkaConfig;
import it.stanislas.kafka.ProducerA;
import org.apache.kafka.streams.KafkaStreams;

public class Delay {
    public static void main(String[] args) {
        final KafkaConfig kafkaConfig = KafkaConfig.buildDefault();

        final KafkaStreams processorStream = ProcessorAtoB.createAndStart(kafkaConfig);

        final ConsumerB consumerB = ConsumerB.createAndStart(kafkaConfig);

        final ProducerA producerA = ProducerA.createAndStart(kafkaConfig);
    }

}