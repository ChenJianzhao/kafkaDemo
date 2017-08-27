package org.demo.topology.impl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.demo.topology.processor.MyProcessor;

import java.util.Map;
import java.util.Properties;

/**
 * Created by cjz on 2017/8/26.
 */

/**
 * 低级别处理器API
 */
public class MyTopologyBuilder {

    public static void main(String[] args) {


        // 【1】低级别处理器API TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", "src-topic")
                .addProcessor("PROCESS1", MyProcessor::new, "SOURCE")
                // create the in-memory state store "COUNTS" associated with processor "PROCESS1"
                .addStateStore(Stores.create("COUNTS").withStringKeys().withStringValues().inMemory().build(), "PROCESS1")

//                .addProcessor("PROCESS2", MyProcessor::new /* the ProcessorSupplier that can generate MyProcessor3 */, "PROCESS1")
//                .addProcessor("PROCESS3", MyProcessor::new /* the ProcessorSupplier that can generate MyProcessor3 */, "PROCESS1")
//
//                // connect the state store "COUNTS" with processor "PROCESS2"
//                .connectProcessorAndStateStores("PROCESS2", "COUNTS")

                .addSink("SINK1", "sink-topic1", "PROCESS1");
//                .addSink("SINK2", "sink-topic2", "PROCESS2")
//                .addSink("SINK3", "sink-topic3", "PROCESS3");

        // 【2】设置 topology 的配置
        // Create an instance of StreamsConfig from the Properties instance
        // Use the configuration to tell your application where the Kafka cluster is,
        // which serializers/deserializers to use by default, to specify security settings,
        // and so on.
        Properties settings = new Properties();

        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Any further settings
//        settings.put(... , ...);
        StreamsConfig config = new StreamsConfig(settings);


        // 【3】启动实例
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

    }
}
