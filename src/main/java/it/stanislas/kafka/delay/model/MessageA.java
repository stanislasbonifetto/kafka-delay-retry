package it.stanislas.kafka.delay.model;

import it.stanislas.kafka.delay.JSONSerdeCompatible;

public class MessageA implements JSONSerdeCompatible {
    private String text;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public MessageA() {}
    public MessageA(final String text) {
        this.text = text;
    }
}
