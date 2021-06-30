package com.poc.kstreams.merge;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

@Slf4j
public class SingleTopicMergerTopology {

    static KafkaStreams getKafkaStreams(String topic, Properties configuration) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Void, String> stream = builder.stream(topic);

        stream.foreach((key, value) -> {

            log.info("Received {} " , value);
        });

        KStream<String, Message> jsonStream = InputStreamToJsonMessageStream.getStringMessageKStream(stream);

        KStream<String, Message> origin1messages = jsonStream.filter((id, message) -> "origin_1".equalsIgnoreCase(message.getOrigin()));
        KStream<String, Message> origin2messages = jsonStream.filter((id, message) -> "origin_2".equalsIgnoreCase(message.getOrigin()));

        // Log messages to see filter work

        origin1messages.foreach((key, value) -> {
            log.info("Received origin1 {} value {} ", key,  value);
        });

        origin2messages.foreach((key, value) -> {
            log.info("Received origin2 {} value {}" , key, value);
        });

        // Store in tables

        KTable<String, Message> origin1Table = origin1messages.toTable(Named.as("origin1Table"), Materialized.with(Serdes.String(), new MessageSerde()));
        KTable<String, Message> origin2Table = origin2messages.toTable(Named.as("origin2Table"), Materialized.with(Serdes.String(), new MessageSerde()));

        KTable<String, Message> joinedTable = JoinedTableHelper.joinedTable(origin1Table, origin2Table);

        joinedTable.toStream().to("merged", Produced.with(Serdes.String(), new MessageSerde()));

        return new KafkaStreams(builder.build(), configuration);

    }

}