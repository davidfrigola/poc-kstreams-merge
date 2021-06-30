package com.poc.kstreams.merge;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.*;

import java.util.Properties;

@Slf4j
public class MergeSingleTopicApplication {

    public static void main(String... args){


        KafkaStreams streams = SingleTopicMergerTopology.getKafkaStreams("tomerge", ConfigurationProvider.configuration("merge-singletopic-app"));

        log.info("Starting kafka streams ...");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }


}
