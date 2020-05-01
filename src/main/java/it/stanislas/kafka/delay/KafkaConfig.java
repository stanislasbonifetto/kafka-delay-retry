package it.stanislas.kafka.delay;

public class KafkaConfig {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SOURCE_TOPIC = "topic-a";
    public static final String DESTINATION_TOPIC = "topic-b";

    private final String bootstrapSever;
    private final String sourceTopic;
    private final String destinationTopic;

    public KafkaConfig(String bootstrapSever, String sourceTopic, String destinationTopic) {
        this.bootstrapSever = bootstrapSever;
        this.sourceTopic = sourceTopic;
        this.destinationTopic = destinationTopic;
    }


    public static KafkaConfig buildDefault() {
        return new KafkaConfig(
                BOOTSTRAP_SERVERS,
                SOURCE_TOPIC,
                DESTINATION_TOPIC
        );
    }

    public String bootstrapSever() {
        return bootstrapSever;
    }

    public String sourceTopic() {
        return sourceTopic;
    }

    public String destinationTopic() {
        return destinationTopic;
    }
}
