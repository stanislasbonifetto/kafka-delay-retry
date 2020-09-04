package it.stanislas.kafka.delay.streamjoin;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ClockKafkaKeyGenerator {
    private final static DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone( ZoneId.systemDefault());

    public String now() {
        return at(Instant.now());
    }

    public String at(final Instant time) {
        final String key = FORMATTER.format(time);
        return key;
    }

}
