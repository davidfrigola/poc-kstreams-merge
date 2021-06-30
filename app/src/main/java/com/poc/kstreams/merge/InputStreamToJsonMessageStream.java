package com.poc.kstreams.merge;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

@Slf4j
public class InputStreamToJsonMessageStream {

    /**
     * Gets a stream with no keys and and string json payload, returns a stream with key = json.id , and json object mapped
     * @param stream input stream
     * @return kstream String,Message
     */
    static KStream<String, Message> getStringMessageKStream(KStream<Void, String> stream) {
        KStream<String, Message> jsonStream = stream.map(new KeyValueMapper<Void, String, KeyValue<String, Message>>() {
            @Override
            public KeyValue<String, Message> apply(Void unused, String s) {
                Message m = JsonMessageMapper.map(s);
                if (m != null) {
                    log.info("Mapping to json object in stream : {}", m.toString());
                    return new KeyValue<String, Message>(m.getId(), m);
                } else {
                    return null;
                }
            }
        });
        return jsonStream;
    }
}