package org.demo.topology.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Created by cjz on 2017/8/26.
 */
public class MyProcessor implements Processor<String, Object> {
    private ProcessorContext context;
    private KeyValueStore kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.context.schedule(1000);
        this.kvStore = (KeyValueStore) context.getStateStore("COUNTS");
    }

    @Override
    public void process(String dummy, Object line) {
        String lineStr = new String((byte[]) line);
        String[] words = lineStr.toLowerCase().split("\\W");

        for (String word : words) {
            String oldValue = (String)this.kvStore.get(word);

            if (oldValue == null) {
                this.kvStore.put(word, String.valueOf(1));
            } else {
                this.kvStore.put(word, String.valueOf(Integer.valueOf(oldValue) + 1));
            }
        }
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, String> iter = this.kvStore.all();

        while (iter.hasNext()) {
            KeyValue<String,String> entry = iter.next();
            context.forward(entry.key.getBytes(), entry.value.toString().getBytes());
        }

        iter.close();
        context.commit();
    }

    @Override
    public void close() {
        this.kvStore.close();
    }
};
