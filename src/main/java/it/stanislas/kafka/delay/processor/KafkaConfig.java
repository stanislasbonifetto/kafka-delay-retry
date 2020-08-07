package it.stanislas.kafka.delay.processor;

import org.immutables.value.Value;

@Value.Immutable
public interface KafkaConfig {

    String BOOTSTRAP_SERVERS = "localhost:9092";
    String SOURCE_TOPIC = "topic-a";
    String DESTINATION_TOPIC = "topic-b";

    String bootstrapSever();
    String sourceTopic();
    String destinationTopic();

    static KafkaConfig buildDefault() {
        return ImmutableKafkaConfig.builder()
                .bootstrapSever(BOOTSTRAP_SERVERS)
                .sourceTopic(SOURCE_TOPIC)
                .destinationTopic(DESTINATION_TOPIC)
                .build();
    }

}
