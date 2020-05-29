package it.stanislas.kafka.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.stanislas.kafka.JSONSerde;
import it.stanislas.kafka.KafkaConfig;
import it.stanislas.kafka.model.MessageA;
import it.stanislas.kafka.model.MessageB;
import it.stanislas.kafka.model.MessageWrapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.Properties;

public class ProcessorAtoB {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static KafkaStreams createAndStart(final KafkaConfig kafkaConfig) {
        final KafkaStreams kafkaStreams = createProcessorStream(kafkaConfig);
        kafkaStreams.start();
        return kafkaStreams;
    }

    public static KafkaStreams createProcessorStream(final KafkaConfig kafkaConfig) {
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<MessageA> messageASerde = new JSONSerde<>();

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-a-to-b-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapSever());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        KStream<String, MessageA> processorStream = builder
                .stream(kafkaConfig.sourceTopic(), Consumed.with(stringSerde, messageASerde));

        TopicNameExtractor<String, MessageWrapper> topicNameExtractor = new TopicNameExtractor<>() {
            @Override
            public String extract(String key, MessageWrapper value, RecordContext recordContext) {
                return kafkaConfig.destinationTopic();
            }
        };

        processorStream
                .mapValues(v -> {

                    final MessageB messageB = new MessageB(v.getFireTime(), v.getText());

                    return new MessageWrapper(v);

//                    if (true) {
//                        return new MessageWrapper<>(v);
//                        throw new RuntimeException();
//                    }

//                    return new MessageWrapper<>(messageB);
                })

                .to(topicNameExtractor);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        return streams;
    }
}