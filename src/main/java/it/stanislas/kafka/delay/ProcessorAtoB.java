package it.stanislas.kafka.delay;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/***
 * with kafka stream consume message A from topic-a and produce message B to topic-b
 */
public class ProcessorAtoB {

    public static KafkaStreams createAndStart(final KafkaConfig kafkaConfig) {
        final KafkaStreams kafkaStreams = createProcessorStream(kafkaConfig);
        kafkaStreams.start();
        return kafkaStreams;
    }

    public static KafkaStreams createProcessorStream(final KafkaConfig kafkaConfig) {
        final StreamsBuilder builder = new StreamsBuilder();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-a-to-b-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapSever());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> processorStream = builder
                .stream(kafkaConfig.sourceTopic(), Consumed.with(stringSerde, stringSerde));

        processorStream
                .mapValues(v -> "original message:" + v)
                .to(kafkaConfig.destinationTopic());

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        return streams;
    }
}
