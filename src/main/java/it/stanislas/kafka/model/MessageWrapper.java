package it.stanislas.kafka.model;

import it.stanislas.kafka.JSONSerdeCompatible;

public class MessageWrapper implements JSONSerdeCompatible {

    private final Message message;

    public MessageWrapper(Message message) {
        this.message = message;
    }

    public Message getMessage() {
        return message;
    }
}
