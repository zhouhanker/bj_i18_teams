package com.cj.dim;

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
 * @Package PACKAGE_NAME.Flink_Cdc
 * @Author chen.jian
 * @Date 2025/4/8 8:41
 * @description: cdc
 */
public class Flink_Cdc {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "double");
        properties.setProperty("time.precision.mode","connect");


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.initial()) // 从最早位点启动
//                .startupOptions(StartupOptions.latest()) // 从最晚位点启动
                .databaseList("gmall_config") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("gmall_config.*") // 设置捕获的表
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();


        DataStreamSource<String> mySQLSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQLSource.print();
//        {"before":null,"after":{"id":2,"name":"ls","age":30},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2025_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744120519189,"transaction":null}
//        {"before":null,"after":{"id":1,"name":"zx","age":20},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2025_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744120519188,"transaction":null}

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh02:9092")
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
