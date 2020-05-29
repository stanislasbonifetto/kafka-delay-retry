package it.stanislas.kafka.delay;

import it.stanislas.kafka.JSONSerde;
import it.stanislas.kafka.KafkaConfig;
import it.stanislas.kafka.model.MessageB;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/***
 * with kafka stream consume message A from topic-a and produce message B to topic-b
 */
public class ProcessorAtoB {

    private static final String SOURCE_TOPIC_NAME = "SourceTopic";
    private static final String PROCESSOR_NAME = "MessageAProcessor";

    public static KafkaStreams createAndStart(final KafkaConfig kafkaConfig) {
        final KafkaStreams kafkaStreams = createProcessorStream(kafkaConfig);
        kafkaStreams.start();
        return kafkaStreams;
    }

    public static KafkaStreams createProcessorStream(final KafkaConfig kafkaConfig) {

        final Serde<String> stringSerde = Serdes.String();
        final Serde<MessageB> messageBSerde = new JSONSerde<>();

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-a-to-b-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapSever());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        StoreBuilder<KeyValueStore<String, MessageB>> messageStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("Messages"),
                stringSerde,
                messageBSerde);


        Topology builder = new Topology();
        builder.addSource(SOURCE_TOPIC_NAME, kafkaConfig.sourceTopic())
                .addProcessor(PROCESSOR_NAME, MessageAProcessor::new, SOURCE_TOPIC_NAME)
                .addStateStore(messageStore, PROCESSOR_NAME)
                .addSink("Sink", kafkaConfig.destinationTopic(), PROCESSOR_NAME);

        final KafkaStreams streams = new KafkaStreams(builder, config);

        return streams;
    }
}
