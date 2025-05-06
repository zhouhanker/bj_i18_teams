package com.flink.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.flink.realtime.dim.test.Test01_FlinkCDC
 * @Author guo.jia.hui
 * @Date 2025/5/5 16:25
 * @description:
 */
public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(3000);
        //TODO 3.使用FlinkCDC读取MySQL表中的数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("flink_realtime") // set captured database
                .tableList("flink_realtime.refund_payment") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.earliest())
                .includeSchemaChanges(true)
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic_db")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        DataStreamSource<String> mySQL_source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQL_source.sinkTo(sink);
        mySQL_source.print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}