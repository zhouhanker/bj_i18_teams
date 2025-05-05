package com.sdy.bean;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * kafka 工具类
 */
public class KafkaUtil {

    private static String brokers = "cdh02:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";

    /**
     * @param topic
     * @return
     */
    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        return sink;
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
