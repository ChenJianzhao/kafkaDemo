package org.demo.topology.impl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.demo.topology.processor.MyProcessor;

import java.util.Map;
import java.util.Properties;

/**
 * Created by cjz on 2017/8/26.
 */
public class MyKStreamBuilder {

    public static void main(String[] args) {

        // 【1】使用 Kafka Streams DSL 构建 topology
        // Use the builders to define the actual processing topology,
        // e.g. to specify from which input topics to read,
        // which stream operations (filter, map, etc.) should be called, and so on.
        KStreamBuilder builder =  new KStreamBuilder();

        KStream<String, String> source1 = builder.stream("topic1", "topic2");
//        KTable<String, GenericRecord> source2 = builder.table("topic3", "stateStoreName");
//        GlobalKTable<String, GenericRecord> source2 = builder.globalTable("topic4", "globalStoreName");

        // 【2】
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


        // 【3】
        KafkaStreams streams = new KafkaStreams(builder, config);
        // Start the Kafka Streams instance
        streams.start();

        // Now generate the data and write to the topic.
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put("key.serializer",
                "org.apache.kafka.common" +
                        ".serialization.StringSerializer");
        producerConfig.put("value.serializer",
                "org.apache.kafka.common" +
                        ".serialization.StringSerializer");

        KafkaProducer producer =
                new KafkaProducer<String, Long>(producerConfig);

        doAgg(source1);

        producer.send(new ProducerRecord("topic1","hello","1"));
        producer.send(new ProducerRecord("topic1","world","1"));
        producer.send(new ProducerRecord("topic1","hey","1"));
        producer.send(new ProducerRecord("topic1","jack","1"));

    }


    public static void doAgg(KStream source1) {
        /**
         * Aggregate
         */
        // written in Java 8+, using lambda expressions
//        KTable<Windowed<String>, Long> counts = source1.groupByKey().aggregate(
//                () -> 0L,  // initial value
//                (aggKey, value, aggregate) -> (Long)aggregate + 1L,   // aggregating value
//                TimeWindows.of(/*"counts", */5000L).advanceBy(1000L), // intervals in milliseconds
//                Serdes.Long() // serde for aggregated value
//        );

        KTable<Windowed<String>, Long> counts = source1.groupByKey().aggregate(
                new Initializer() {
                    @Override
                    public Object apply() {
                        return 1L;
                    }
                },
                new Aggregator() {
                    @Override
                    public Object apply(Object key, Object value, Object aggregate) {
                        return Long.valueOf(aggregate.toString()) + 1L;
                    }
                },
                TimeWindows.of(/*"counts", */5000L).advanceBy(1000L), // intervals in milliseconds
                Serdes.Long() // serde for aggregated value
        );

        counts.to("topic1-output");
    }

    public static void processStreamWithDSL(KStream source1, KTable source2) {


        /**
         * Join
         *
         * 这个就日了狗了
         * Lambda表达式自身不能指定类型参数。
         * (当然，由于存在类型推断，所有Lambda表达式都展现出了一些类似于泛型的特征)。
         * 但是，与Lambda表达式关联的函数式接口可以泛型。此时，Lambda表达式的目标类型部分由声明函数式接口引用时指定的参数类型决定。
         */

        ValueJoiner<Map, Map, String> joiner = (record1, record2) -> record1.get("user") + "-" + record2.get("region");
        KStream<String, String> joined = source1.leftJoin(source2,joiner);


        /**
         * equivalent to
         *
         * 如果已经通过上面的to方法写入到一个主题中，
         * 但是如果你还需要继续读取和处理这些消息，可以从输出主题构建一个新流，Kafka Streams提供了一个便利的方法，through:
         * materialized = builder.stream("topic4");
         */

//        joined.to("topic4");
        KStream materialized = joined.through("topic4");
    }
}
