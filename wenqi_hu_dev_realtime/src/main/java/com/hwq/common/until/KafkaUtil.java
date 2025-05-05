package com.hwq.common.until;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


public class KafkaUtil {

    private static String brokers = "cdh01:9092,cdh02:9092,cdh03:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";


    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(brokers,
                topic,
                new SimpleStringSchema());
    }


    public static DataStreamSource<String> getKafkaSource(StreamExecutionEnvironment env, String topic, String groupId) {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
      return   env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    //设置kafka参数
//    public static String getKafkaDDL(String topic, String groupId) {
//        return  " 'connector' = 'kafka', " +
//                " 'topic' = '" + topic + "'," +
//                " 'properties.bootstrap.servers' = '" + brokers + "', " +
//                " 'properties.group.id' = '" + groupId + "', " +
//                " 'format' = 'json', " +
//                " 'scan.startup.mode' = 'latest-offset'  ";
//    }

}
