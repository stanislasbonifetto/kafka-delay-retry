package it.stanislas.kafka.delay.streamjoin

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.StreamJoined
import java.time.Duration
import java.util.*


class DelayStream (private val kafkaStreams: KafkaStreams) {

    companion object {
        fun buildAndStart(
            bootstrapServers: String,
            delayTopicName: String,
            tickTocTopicName: String,
            firedTopicName: String
        ): DelayStream {
            val delayStream = DelayStream(
                buildStream(
                    bootstrapServers,
                    delayTopicName,
                    tickTocTopicName,
                    firedTopicName
                )
            )
            delayStream.kafkaStreams.start()
            return DelayStream(
                buildStream(
                    bootstrapServers,
                    delayTopicName,
                    tickTocTopicName,
                    firedTopicName
                )
            )
        }

        fun buildStream(
            bootstrapServers: String,
            delayTopicName: String,
            clockTopicName: String,
            firedTopicName: String
        ): KafkaStreams {
            val config = Properties()
            config[StreamsConfig.APPLICATION_ID_CONFIG] = "delay-stream"
            config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
            config[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
            config[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
            val builder = StreamsBuilder()
            val clockStream = builder.stream(
                clockTopicName,
                Consumed.with(Serdes.String(), Serdes.String())
            )
            val delayedStream = builder.stream(
                delayTopicName,
                Consumed.with(Serdes.String(), Serdes.String())
            )

            //group by key the Clock stream because for resilience multiple producers send a clock event
            val groupByClockStream = clockStream.groupByKey()
                .reduce { clock1, _ -> clock1 }.toStream()
            delayedStream.join(
                groupByClockStream,
                { delayedValue, _ -> delayedValue },  //A dey window to allow to delay event at max 1 day
                JoinWindows.of(Duration.ofDays(1)),
                StreamJoined.with(
                    Serdes.String(),
                    Serdes.String(),
                    Serdes.String()
                )
            )
                .map { _, value ->
                    val values = value.split(":").toTypedArray()
                    val delayKey = values[0]
                    val delayMessage = values[1]
                    KeyValue(delayKey, delayMessage)
                }
                .to(firedTopicName)
            val topology = builder.build()
            return KafkaStreams(topology, config)
        }
    }

}