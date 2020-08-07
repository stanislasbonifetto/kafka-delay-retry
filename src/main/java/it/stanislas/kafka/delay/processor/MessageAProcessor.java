package it.stanislas.kafka.delay.processor;

import it.stanislas.kafka.delay.processor.model.MessageA;
import it.stanislas.kafka.delay.processor.model.MessageB;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

public class MessageAProcessor implements Processor<String, MessageA> {

    private ProcessorContext context;
    private KeyValueStore<String, MessageB> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        kvStore = (KeyValueStore) context.getStateStore("Messages");

        this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            KeyValueIterator<String, MessageB> iter = this.kvStore.all();
            while (iter.hasNext()) {
                KeyValue<String, MessageB> entry = iter.next();
                if (Instant.now().toEpochMilli() > entry.value.getMessageATimestamp()) {
                    context.forward(entry.key, entry.value);
                    kvStore.delete(entry.key);
                }
            }
            iter.close();
            context.commit();
        });
    }

    @Override
    public void process(String key, MessageA messageA) {
        final MessageB messageB = new MessageB(messageA.getFireTime(), messageA.getText());
        this.kvStore.put(key, messageB);
    }

    @Override
    public void close() {
        // nothing to do
    }

}