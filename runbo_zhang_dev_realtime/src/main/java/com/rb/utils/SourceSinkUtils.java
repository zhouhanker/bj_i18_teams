package com.rb.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Package com.rb.CdcSource
 * Author runbo.zhang
 * Date 2025/4/7 20:55
 * description: 1
 */
public class SourceSinkUtils {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> kafkaRead = kafkaRead(env, "log_topic_flink_online_v1_log");
        kafkaRead.print();
        env.execute();
    }

    public static DataStreamSource<String> cdcRead(StreamExecutionEnvironment env, String dbName, String tableName) throws Exception {


        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
//        env.enableCheckpointing(3000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");


        // 设置 source 节点的并行度为 4
        // 设置 sink 节点并行度为 1


        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "double");
        properties.setProperty("time.precision.mode", "connect");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList(dbName) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(dbName + "." + tableName) // 设置捕获的表
                .username("root")
                .password("root")
                .startupOptions(StartupOptions.earliest())
//                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        return mySQLSource;
    }

    public static KafkaSink<String> sinkToKafka(String topic_name) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic_name)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        return sink;


    }
    public static DataStreamSource<String>  kafkaRead(StreamExecutionEnvironment env,String topic ){

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setTopics(topic)
                .setGroupId(topic+"1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        return kafkaSource;
    }
    public static DorisSink<String> getDorisSink(String db,String tableName){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes("cdh03:8030")
                        .setTableIdentifier( db+ "." + tableName)
                        .setUsername("root")
                        .setPassword("root")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferSize(1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }
}
