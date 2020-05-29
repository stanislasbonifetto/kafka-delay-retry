package it.stanislas.kafka.model;

import it.stanislas.kafka.JSONSerdeCompatible;

//FIXME: use Immutable
public class MessageB extends Message implements JSONSerdeCompatible {
    private Long messageATimestamp;
    private String text;

    public Long getMessageATimestamp() {
        return messageATimestamp;
    }

    public String getText() {
        return text;
    }

    public MessageB() {
    }

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