package it.stanislas.kafka.delay.model;

import org.junit.Assert;
import org.junit.Test;

public class FoobarValueTest {

    @Test
    public void testAnimal() {
        FoobarValue value = ImmutableFoobarValue.builder()
                .foo(2)
                .bar("Bar")
                .build();

        Assert.assertEquals(2, value.foo());
        Assert.assertEquals("Bar", value.bar());
    }
}
