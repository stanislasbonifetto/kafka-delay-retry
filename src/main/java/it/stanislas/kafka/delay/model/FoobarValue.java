package it.stanislas.kafka.delay.model;

import org.immutables.value.Value;

import java.util.List;
import java.util.Set;

@Value.Immutable
public interface FoobarValue {
    int foo();
    String bar();
}