package it.stanislas.kafka.delay.streamjoin

import io.reactivex.rxjava3.core.Observable
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.StreamsConfig
import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.TimeUnit


@Testcontainers
class DelayWithStreamsTest {
    private val clockKafkaKeyGenerator = ClockKafkaKeyGenerator()

    @Test
    fun delay_a_message() {

        //given
        val delayStream = DelayStream.buildAndStart(
            bootstrapServers,
            DELAY_TOPIC_NAME,
            CLOCK_TOPIC_NAME,
            FIRED_TOPIC_NAME
        )
        val clockProducer = ClockProducer.buildAndStart(
            bootstrapServers,
            CLOCK_TOPIC_NAME,
            clockKafkaKeyGenerator
        )
        val delayProducer = DelayProducer.build(
            bootstrapServers,
            DELAY_TOPIC_NAME,
            clockKafkaKeyGenerator
        )
        val firedConsumer = buildKafkaConsumer(
            bootstrapServers,
            FIRED_TOPIC_NAME
        )

        //when
        // I fire a message with 10s delay
        val now = Instant.now()
        val fireAt = now.plus(10, ChronoUnit.SECONDS)
        val messageKey = "my-key"
        val messageValue = "my-value"

        // I set the message value with format "key:value"
        val message = "$messageKey:$messageValue"
        delayProducer.sendAt(message, fireAt)

        //then
        // I consume the fired topic to check if the message is fired and check the timestamp of the message
        Awaitility.await().atMost(1, TimeUnit.MINUTES).until {
            val records = firedConsumer.poll(Duration.ofSeconds(1))
            records.forEach { r ->
                println(
                    "fired k:" + r.key() + " v:" + r.value()
                )
            }
            assertMessageIsFiredAtTheRightTime(records, messageKey, messageValue, fireAt)
        }
    }

    @Test
    @Disabled //this test is to just run and print it doesn't any validation
    fun delay_messages() {
        //given
        val delayStream = DelayStream.buildAndStart(
            bootstrapServers,
            DELAY_TOPIC_NAME,
            CLOCK_TOPIC_NAME,
            FIRED_TOPIC_NAME
        )
        val clockProducer = ClockProducer.buildAndStart(
            bootstrapServers,
            CLOCK_TOPIC_NAME,
            clockKafkaKeyGenerator
        )
        val delayProducer = DelayProducer.build(
            bootstrapServers,
            DELAY_TOPIC_NAME,
            clockKafkaKeyGenerator
        )
        val firedConsumer = buildKafkaConsumer(
            bootstrapServers,
            FIRED_TOPIC_NAME
        )

        //when
        sendAMessageDelayedOneMinutesEachThenSeconds(delayProducer)

        //then
        Observable
            .interval(1, TimeUnit.SECONDS)
            .doOnNext {
                val records: ConsumerRecords<String, String> =
                    firedConsumer.poll(Duration.ofSeconds(1))
                records.forEach{ r ->
                    println(
                        "fired k:" + r.key() + " v:" + r.value()
                    )
                }
            }
            .subscribe()
        try {
            Thread.sleep(10 * 60 * 1000.toLong())
        } catch (e: Exception) {
        }
    }

    private fun buildKafkaConsumer(
        bootstrapServers: String,
        topicName: String
    ): Consumer<String, String> {
        val props = Properties()
        val uuid = UUID.randomUUID()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "consumer-$uuid"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"

        // Create the consumer using props.
        val consumer = KafkaConsumer<String, String>(props)

        // Subscribe to the topic.
        consumer.subscribe(listOf(topicName))
        return consumer
    }

    private fun sendAMessageDelayedOneMinutesEachThenSeconds(delayProducer: DelayProducer) {
        Observable
            .interval(10, TimeUnit.SECONDS)
            .doOnNext { n ->
                val now = Instant.now()
                val fireAt = now.plus(1, ChronoUnit.MINUTES)
                val value = n.toString()
                val recordMetadataFeature = delayProducer.sendAt(value, fireAt)
            }
            .subscribe()
    }

    companion object {
        @Container
        val KAFKA_CONTAINER = KafkaContainer()
        lateinit var bootstrapServers: String

        //    final static String bootstrapServers = "localhost:9092";
        // topic-delay
        const val DELAY_TOPIC_NAME = "delay"

        // topic-clock
        const val CLOCK_TOPIC_NAME = "clock"

        // topic-fired
        const val FIRED_TOPIC_NAME = "fired"

        @BeforeAll
        @JvmStatic
        fun setup() {
            //given
            KAFKA_CONTAINER.start()
            createTopics()
        }

        private fun createTopics() {
            bootstrapServers =
                KAFKA_CONTAINER.bootstrapServers
            val config = Properties()
            config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
            val adminClient = AdminClient.create(config)
            val delayTopic = NewTopic(DELAY_TOPIC_NAME, 1, 1.toShort())
            val clockTopic = NewTopic(CLOCK_TOPIC_NAME, 1, 1.toShort())
            val firedTopic = NewTopic(FIRED_TOPIC_NAME, 1, 1.toShort())
            adminClient.createTopics(listOf(delayTopic, clockTopic, firedTopic))
            adminClient.close()
        }

        private fun assertMessageIsFiredAtTheRightTime(
            records: ConsumerRecords<String, String>,
            expectedKey: String,
            expectedValue: String,
            expectedAt: Instant
        ): Boolean {
            if (records.count() <= 0) return false
            val record = records.iterator().next()
            val recordTimestamp = Instant.ofEpochMilli(record.timestamp())
            val diffFireTime = Duration.between(expectedAt, recordTimestamp)
            val diffFireTimeInSeconds = diffFireTime.seconds
            println("diff :" + diffFireTime.seconds)
            val keyIsEqual = record.key() == expectedKey
            val valueIsEqual = record.value() == expectedValue
            val isFireOnTime = diffFireTimeInSeconds > -2 && diffFireTimeInSeconds < 2

//        System.out.println("keyIsEqual :" + keyIsEqual + " valueIsEqual :" + valueIsEqual + " isFireOnTime: "+ isFireOnTime);
            return keyIsEqual && valueIsEqual && isFireOnTime
        }
    }
}