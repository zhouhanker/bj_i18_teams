package com.bg;

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
 * @Package com.bg.FlinkCDC
 * @Author Chen.Run.ze
 * @Date 2025/4/7 19:31
 * @description: Flink CDC
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        prop.put("useSSL","false");

        prop.put("decimal.handling.mode","double");
        prop.put("time.precision.mode","connect");

        // 指定分块键列
        prop.setProperty("scan.incremental.snapshot.chunk.key-column", "id");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("gmall2024") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("gmall2024.*") // 设置捕获的表
                .username("root")
                .password("root")
                .debeziumProperties(prop)
                .startupOptions(StartupOptions.latest()) // 从最早位点启动
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        {"before":null,"after":{"id":389,"order_id":191,"order_status":"1002","create_time":1744061980000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744033180000,"snapshot":"false","db":"gmall2024","sequence":null,"table":"order_status_log","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":5083465,"row":0,"thread":280,"query":null},"op":"c","ts_ms":1744077706871,"transaction":null}
        mySQLSource.print();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic_db")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        mySQLSource.sinkTo(sink);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
