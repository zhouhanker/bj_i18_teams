package dim;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.cm.dim.function.Test01_FlinkCDC
 * @Author chen.ming
 * @Date 2025/5/2 14:33
 * @description:
 */
public class Test01_FlinkCDC {
    @SneakyThrows
    public static void main(String[] args)  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度
        env.setParallelism(4);

        // 配置 MySQL 连接属性
        Properties prop = new Properties();
        prop.put("useSSL","false");
        prop.put("decimal.handling.mode","double");
        prop.put("time.precision.mode","connect");
        prop.setProperty("scan.incremental.snapshot.chunk.key-column","id");//提高并发 分片主键 控制快照分片粒度
        // 构建 MySQL CDC 源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.160.60.17")
                .port(3306)
                .debeziumProperties(prop)
                .databaseList("realtime_v1") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("realtime_v1.*") // 设置捕获的表
                .username("root")
                .password("Zh1028,./")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();


        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);


        //从源创建数据流
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQLSource.print();
        //输出的数据
        //{"before":null,"after":{"id":29,"spu_id":10,"price":"RQ==","sku_name":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇","sku_desc":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇","weight":"ZA==","tm_id":9,"category3_id":477,"sku_default_img"
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("chenming_db")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        mySQLSource.sinkTo(sink);

        env.execute("flink_cdc");
    }
}