package com.poc.kstreams.merge;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

@Slf4j
public class JoinedTableHelper {
    static KTable<String, Message> joinedTable(KTable<String, Message> origin1Table, KTable<String, Message> origin2Table) {
        KTable<String, Message> joinedTable = origin2Table.join(origin1Table, new ValueJoiner<Message, Message, Message>() {
            @Override
            public Message apply(Message message, Message message2) {
                if (message.getId().equalsIgnoreCase(message2.getId())) { // this should be always true as we have rekeyed with ID
                    log.info("Joining id {} : {} <> {}", message.getId(), message.toString(), message2.toString());
                    Message merged = Message.builder()
                            .id(message.getId())
                            .origin("merger")
                            .value1(message2.getValue1())
                            .value2(message.getValue2())
                            .value3(message.getValue3())
                            .build();
                    log.info("Merged message {} ", merged);
                    return merged;
                } else {
                    log.info("Joined 2 elements with different ID - something went wrong in rekeying");
                    return null;
                }


            }
        });
        return joinedTable;
    }
}