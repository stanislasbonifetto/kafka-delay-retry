package it.stanislas.kafka.delay.processor.model;

import it.stanislas.kafka.delay.processor.JSONSerdeCompatible;

//FIXME: use Immutable
public class MessageB implements JSONSerdeCompatible {
    private Long messageATimestamp;
    private String text;

    public Long getMessageATimestamp() {
        return messageATimestamp;
    }

    public String getText() {
        return text;
    }

    public MessageB() {}

    public MessageB(Long messageATimestamp, String text) {
        this.messageATimestamp = messageATimestamp;
        this.text = text;
    }

    @Override
    public String toString() {
        return "MessageB{" +
                "messageATimestamp=" + messageATimestamp +
                ", text='" + text + '\'' +
                '}';
    }
}