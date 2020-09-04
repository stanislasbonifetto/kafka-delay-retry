package it.stanislas.kafka.delay.streamjoin;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.Properties;

public class DelayStream {

    final private KafkaStreams kafkaStreams;

    protected DelayStream(final KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    public static DelayStream buildAndStart(final String bootstrapServers, final String delayTopicName, final String tickTocTopicName, final String firedTopicName) {
        final DelayStream delayStream = new DelayStream(buildStream(bootstrapServers, delayTopicName, tickTocTopicName, firedTopicName));

        delayStream.kafkaStreams.start();

        return new DelayStream(buildStream(bootstrapServers, delayTopicName, tickTocTopicName, firedTopicName));
    }


    protected static KafkaStreams buildStream(final String bootstrapServers, final String delayTopicName, final String clockTopicName, final String firedTopicName) {

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "delay-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> clockStream = builder.stream(clockTopicName, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> delayedStream = builder.stream(delayTopicName, Consumed.with(Serdes.String(), Serdes.String()));

        //group by key the Clock stream because for resilience multiple producers send a clock event
        KStream<String, String> groupByClockStream = clockStream.groupByKey().reduce((clock1, clock2) -> clock1).toStream();

        delayedStream.join(
                    groupByClockStream,
                    (delayedValue, clockValue) ->  delayedValue,
                    //A dey window to allow to delay event at max 1 day
                    JoinWindows.of(Duration.ofDays(1)),
                    StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                )
                .map((key, value) -> {
                    String[] values = value.split(":");
                    final String delayKey = values[0];
                    final String delayMessage = values[1];
                    return new KeyValue<>(delayKey, delayMessage);
                })
                .to(firedTopicName);

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, config);
        return streams;
    }

}
