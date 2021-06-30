package com.poc.kstreams.merge;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

@Slf4j
public class MergeTwoTopicsApplication {

    public static void main(String... args){


        KafkaStreams streams = TwoTopicsMergerTopology.getKafkaStreams("tomerge1", "tomerge2",ConfigurationProvider.configuration("merge-twotopics" +
                "-app"));

        log.info("Starting kafka streams ...");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }


}
