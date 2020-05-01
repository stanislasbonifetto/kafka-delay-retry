package it.stanislas.kafka.delay.model;

public class MessageB {
    final private Long messageATimestamp;
    final private String text;

    public MessageB(Long messageATimestamp, String text) {
        this.messageATimestamp = messageATimestamp;
        this.text = text;
    }
}