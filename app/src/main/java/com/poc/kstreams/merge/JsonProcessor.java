package com.poc.kstreams.merge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class JsonProcessor implements Processor<Void, String, Void,Void> {
    ObjectMapper mapper = new ObjectMapper();

    public void init(ProcessorContext<Void, Void> context) {

    }

    public void process(Record<Void, String> record) {
        try {
            Message message = mapper.readValue(record.value(), Message.class); //mapper.convertValue(record.value(), Message.class);
            System.out.println("Message id " + message.getId());
        } catch(Exception e) {
            System.out.println("Invalid json found : " + record.value());
            e.printStackTrace();
        }
    }

    public void close() {

    }
}
