package com.zsf.retail_v1.realtime.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * kafka 工具类
 */
public class KafkaUtil {

    private static String brokers = "cdh01:9092,cdh02:9092,cdh03:9092";

    /**
     * @param topic
     * @return
     */
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

}
