package it.stanislas.kafka.model;

import org.immutables.value.Value;

@Value.Immutable
public interface FoobarValue {
    int foo();
    String bar();
}