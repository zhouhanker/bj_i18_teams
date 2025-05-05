package realtime_Dim;

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
 * @Author a_yang
 * @Date 2025/4/8 9:31
 * @description: cdc
 */
public class Flink_Cdc {
    //kafka   节点
    private static String brokers = "cdh01:9092";
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","string");
        properties.setProperty("time.precision.mode","connect");
//        properties.setProperty("scan.incremental.snapshot.chunk.key-column", "gmall_config.base_region");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
//                .startupOptions(StartupOptions.earliest())
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(properties)
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.*")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(1);// 设置 sink 节点并行度为 1
        //c 添加
        //d 删除
        //修改 u
        //r 读
//        {"before":null,"after":{"id":2944,"order_id":1504,"order_status":"1001","create_time":1744068523000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744100949000,"snapshot":"false","db":"realtime","sequence":null,"table":"order_status_log","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":2446435,"row":0,"thread":177,"query":null},"op":"c","ts_ms":1744100949425,"transaction":null}
        ds.print();
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic_db")
//                               realtime_v1_table_all_mysql_v2
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        ds.sinkTo(sink);
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
