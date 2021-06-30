package com.poc.kstreams.merge;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class LogProcessor implements Processor<String, String, Void,Void> {

    public void init(ProcessorContext<Void, Void> context) {

    }

    public void process(Record<String, String> record) {
        System.out.println("Received " + record.key() + " - " + record.value());
    }

    public void close() {

    }
}
