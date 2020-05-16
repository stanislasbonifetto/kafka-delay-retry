package it.stanislas.kafka.delay;


import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

public class KafkaConfigTest {

    @Test
    public void test_default_conf() {
        final KafkaConfig kafkaConfig = KafkaConfig.buildDefault();

        assertEquals(KafkaConfig.BOOTSTRAP_SERVERS, kafkaConfig.bootstrapSever());
        assertEquals(KafkaConfig.SOURCE_TOPIC, kafkaConfig.sourceTopic());
        assertEquals(KafkaConfig.DESTINATION_TOPIC, kafkaConfig.destinationTopic());
    }
}
