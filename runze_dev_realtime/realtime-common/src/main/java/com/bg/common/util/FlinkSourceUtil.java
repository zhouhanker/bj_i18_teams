package com.bg.common.util;

import com.bg.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

/**
 * @Package com.bg.common.util.FlinkSourceUtil
 * @Author Chen.Run.ze
 * @Date 2025/4/9 13:52
 * @description: 获取相关Source
 */
public class FlinkSourceUtil {
    //获取Kafka Source
    public static KafkaSource<String> getKafkaSource(String topic,String group){
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(group)
                //在消费断 , 需要设置消费的隔离级别为读已提交
//                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                //保证消费的精准一次性，需要手动维护偏移量
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setStartingOffsets(OffsetsInitializer.earliest())
                //注意：如果使用Flink提供的反序列化，消息如果为空，会报错
//                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) {
                        if (bytes != null){
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
    }

    //获取MysqlSource
    public static MySqlSource<String> getMysqlSource(String database,String tableName){
        Properties props = new Properties();
        props.put("useSSL","false");
        props.put("decimal.handling.mode","double");
        props.put("time.precision.mode","connect");
        props.setProperty("allowPublicKeyRetrieval", "true");

        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(database) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(database + "." + tableName) // 设置捕获的表
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .debeziumProperties(props)
                .startupOptions(StartupOptions.initial()) // 从最早位点启动
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
    }
}
