import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.*;

/**
 * Demonstrates tumbling windows.
 *
 * @author Timothy Renner
 */
public class TumblingWindowKafkaStream {

    /**
     * Runs the streams program, writing to the "long-counts-all" topic.
     *
     * @param args Not used.
     */
    public static void main(String[] args) throws Exception {

        createStream();

        startConsumer();

        startProducer();

    }

    private static void createStream() {
        final StreamsBuilder builder = new StreamsBuilder();

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "tumbling-window-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();


        KStream<Long, String> longs = builder.stream("longs", Consumed.with(longSerde, stringSerde));

        KTable<Long, String> longCounts = longs.groupByKey()
                .aggregate(() -> "", (key, value, aggregate) -> aggregate.concat(";").concat(value));


        // The tumbling windows will clear every ten seconds.
//        KTable<Windowed<Long>, Long> longCounts =
//                longs
//                        .groupBy((key, value) -> key)
//                        .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
//                        .count(Named.as("long-counts"));

        longCounts
                .toStream()
                .to("long-counts-all");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    private static void startProducer() {
        // Now generate the data and write to the topic.
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<String, String>(producerConfig);

        long time = System.currentTimeMillis();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            private int counter = 0;
            private int delta = 0;

            @Override
            public void run() {
                //                final UUID uuid = UUID.randomUUID();
//                final String key = uuid.toString();

                if (counter % 5 == 0) {
                    delta += 5000;
                }

                final long key = time + delta;
                final String message = "ping " + counter;


                producer.send(new ProducerRecord("longs", key, message));
                System.out.println("send message k:" + key + " v:" + message);
                counter++;
            }
        }, 0, 1000);

    }

    private static void startConsumer() {
        Thread t = new Thread(() -> {
            final Consumer<String, Long> consumer = createConsumer();
            while (true) {
                final ConsumerRecords<String, Long> records = consumer.poll(Duration.ofSeconds(1));
                System.out.println("pull new messages");
                records.forEach(r -> {
                    System.out.println("Aggregation k:" + r.key() + " v:" + r.value());
                });
            }
        });
        t.start();
    }

    private static Consumer<String, Long> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, Long> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList("long-counts-all"));
        return consumer;
    }

}