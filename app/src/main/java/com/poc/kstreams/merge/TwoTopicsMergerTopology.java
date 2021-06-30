package com.poc.kstreams.merge;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class TwoTopicsMergerTopology {

    static KafkaStreams getKafkaStreams(String topic1, String topic2, Properties configuration) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Void, String> streamTopic1 = builder.stream(topic1);
        KStream<Void, String> streamTopic2 = builder.stream(topic2);

        KStream<String,Message> jsonStreamTopic1 = InputStreamToJsonMessageStream.getStringMessageKStream(streamTopic1);
        KStream<String,Message> jsonStreamTopic2 = InputStreamToJsonMessageStream.getStringMessageKStream(streamTopic2);


        KTable<String, Message> origin1Table = jsonStreamTopic1.toTable(Named.as("twotopics_origin1Table"), Materialized.with(Serdes.String(), new MessageSerde()));
        KTable<String, Message> origin2Table = jsonStreamTopic2.toTable(Named.as("twotopics_origin2Table"), Materialized.with(Serdes.String(), new MessageSerde()));

        KTable<String, Message> joinedTable = JoinedTableHelper.joinedTable(origin1Table,origin2Table);

        joinedTable.toStream().to("twotopics_merged", Produced.with(Serdes.String(), new MessageSerde()));

        return new KafkaStreams(builder.build(), configuration);

    }
}
