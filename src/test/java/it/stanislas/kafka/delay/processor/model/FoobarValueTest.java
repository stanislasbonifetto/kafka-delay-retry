package it.stanislas.kafka.delay.processor.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FoobarValueTest {

    @Test
    public void testAnimal() {
        FoobarValue value = ImmutableFoobarValue.builder()
                .foo(2)
                .bar("Bar")
                .build();

        assertEquals(2, value.foo());
        assertEquals("Bar", value.bar());
    }
}
