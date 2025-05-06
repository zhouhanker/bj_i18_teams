package com.realtime_dim;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.bw.FlinkCDC_mysqltokafka
 * @Author chenghang.wang
 * @Date 2025/4/9 18:58
 * @description: dad
 */
public class FlinkCDC_mysqltokafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","string");
        properties.setProperty("time.precision.mode","connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .port(3306)
                .databaseList("realtime")
                .tableList("realtime.*")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStreamSource<String> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(1);// 设置 sink 节点并行度为 1
//        {"before":null,"after":{"id":2944,"order_id":1504,"order_status":"1001","create_time":1744068523000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744100949000,"snapshot":"false","db":"realtime","sequence":null,"table":"order_status_log","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":2446435,"row":0,"thread":177,"query":null},"op":"c","ts_ms":1744100949425,"transaction":null}
        ds.print();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("realtime_v1_table_all_mysql")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        ds.sinkTo(sink);
        env.execute("Print MySQL Snapshot + Binlog");



    }
}
