package it.stanislas.kafka.delay;

import org.junit.Assert;
import org.junit.Test;

public class KafkaConfigTest {

    @Test
    public void test_default_conf() {
        final KafkaConfig kafkaConfig = KafkaConfig.buildDefault();

        Assert.assertEquals(KafkaConfig.BOOTSTRAP_SERVERS, kafkaConfig.bootstrapSever());
        Assert.assertEquals(KafkaConfig.SOURCE_TOPIC, kafkaConfig.sourceTopic());
        Assert.assertEquals(KafkaConfig.DESTINATION_TOPIC, kafkaConfig.destinationTopic());
    }
}
