package it.stanislas.kafka.delay;

import it.stanislas.kafka.delay.model.MessageA;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/***
 * produce a message A in topic-a
 */
public class ProducerA {


    private KafkaProducer producerA;

    public ProducerA() {
        this.producerA = new KafkaProducer<String, String>(createProducerConfig());
    }

    public void startProducer() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            private int counter = 0;

            @Override
            public void run() {

                final String key = "message-a-key";
                final MessageA message = new MessageA("message-a-" + counter);

                producerA.send(new ProducerRecord("topic-a", key, message.getText()));
                System.out.println("send message k:" + key + " v:" + message.getText());
                counter++;
            }
        }, 0, 1000);
    }


    private Properties createProducerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return producerConfig;
    }
}
