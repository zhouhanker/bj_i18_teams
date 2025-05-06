package com.retailersv1;

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
public class FlinkCDC_mysqltokafka {

        //kafka   节点
        private static String brokers = "cdh02:9092";
        public static void main(String[] args) throws Exception {
            Properties properties = new Properties();
            properties.setProperty("decimal.handling.mode","string");
            properties.setProperty("time.precision.mode","connect");
            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                    .hostname("10.160.60.17")
                    .startupOptions(StartupOptions.initial())
                    .debeziumProperties(properties)
                    .port(3306)
                    .databaseList("realtime_v1")
                    .tableList("realtime_v1.*")
                    .username("root")
                    .password("Zh1028,./")
                    .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                    .build();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
            DataStreamSource<String> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source56")
                    // 设置 source 节点的并行度为 4
                    .setParallelism(1);// 设置 sink 节点并行度为 1
            ds.print();
//        {"before":null,"after":{"id":2944,"order_id":1504,"order_status":"1001","create_time":1744068523000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744100949000,"snapshot":"false","db":"realtime","sequence":null,"table":"order_status_log","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":2446435,"row":0,"thread":177,"query":null},"op":"c","ts_ms":1744100949425,"transaction":null}


            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("xuanye_chang_Business")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();


            ds.sinkTo(sink);

            env.execute("Print MySQL Snapshot + Binlog");

    }
}
