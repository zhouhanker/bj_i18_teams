package com.sdy.ods.App;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.sdy.Test
 * @Author danyu-shi
 * @Date 2025/4/8 14:50
 * @description:
 */
public class flinkcdc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        env.setParallelism(4);

        Properties prop = new Properties();
        prop.put("useSSL","false");
        prop.put("decimal.handling.mode","double");
        prop.put("time.precision.mode","connect");
        prop.setProperty("scan.incremental.snapshot.chunk.key-column", "id");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.160.60.17")
                .port(3306)
                .databaseList("realtime_v1") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
//                .tableList("realtime_v1.order_refund_info") // 设置捕获的表
                 .tableList("realtime_v1.*") // 设置捕获的表
                .username("root")
                .password("Zh1028,./")
                .startupOptions(StartupOptions.initial())  // 从最早位点启动
//               .startupOptions(StartupOptions.latest()) // 从最晚位点启动
                .debeziumProperties(prop)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStream<String> mySQLSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//
        mySQLSource.print();
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("stream-dev2-danyushi")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        mySQLSource.sinkTo(sink);


        env.execute("Print MySQL Snapshot + Binlog");
    }
}
