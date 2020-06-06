package it.stanislas.kafka.delay;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import it.stanislas.kafka.delay.model.MessageA;
import it.stanislas.kafka.delay.model.MessageB;

@SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
@JsonSubTypes({
        @JsonSubTypes.Type(value = MessageA.class, name = "message_a"),
        @JsonSubTypes.Type(value = MessageB.class, name = "message_b")
})
public interface JSONSerdeCompatible {
}