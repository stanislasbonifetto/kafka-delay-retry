package it.stanislas.kafka.delay.processor.model;

import it.stanislas.kafka.delay.processor.JSONSerdeCompatible;

//FIXME: use Immutable
public class MessageA implements JSONSerdeCompatible {

    private String text;

    private long fireTime;

    public long getFireTime() {
        return fireTime;
    }

    public void setFireTime(long fireTime) {
        this.fireTime = fireTime;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public MessageA() {
    }

    public MessageA(String text, long fireTime) {
        this.text = text;
        this.fireTime = fireTime;
    }
}
