import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/** Demonstrates tumbling windows.
 *
 * @author Timothy Renner
 */
public class TumblingWindowKafkaStream {
    
    /** Runs the streams program, writing to the "long-counts-all" topic.
     *
     * @param args Not used.
     */
    public static void main(String[] args) throws Exception {

        final StreamsBuilder builder = new StreamsBuilder();

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
            "tumbling-window-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092");
        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStream<String, Long> longs = builder.stream("longs", Consumed.with(stringSerde, longSerde));

        // The tumbling windows will clear every ten seconds.
        KTable<Windowed<String>, Long> longCounts =
            longs.groupByKey()
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
                    .count(Named.as("long-counts"));

        longCounts.toStream().to("long-counts-all");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        //
        Thread t = new Thread(() -> {
            final Consumer<String, Long> consumer = createConsumer();
            while(true) {
                final ConsumerRecords<String, Long> records = consumer.poll(1000);
                System.out.println("pull new messages");
                records.forEach(r -> {
                    System.out.println("create aggregation k:" + r.key() + " v:" + r.value());
                });
            }
        });
        t.start();

        // Now generate the data and write to the topic.
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer",
                "org.apache.kafka.common.serialization.LongSerializer");

        KafkaProducer producer = 
            new KafkaProducer<String, Long>(producerConfig);

        Random rng = new Random(12345L);

        while(true) {
            final String key = "A";
            final long count = Math.abs(rng.nextLong()%10);

            producer.send(new ProducerRecord<String, Long>(
                "longs", key, count));
            System.out.println("send message k:" + key + " v:" + count);

            Thread.sleep(500L);
        } // Close infinite data generating loop.
    } // Close main.

    private static Consumer<String, Long> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.LongDeserializer");

        // Create the consumer using props.
        final Consumer<String, Long> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList("long-counts-all"));
        return consumer;
    }

} // Close TumblingWindowKafkaStream.
