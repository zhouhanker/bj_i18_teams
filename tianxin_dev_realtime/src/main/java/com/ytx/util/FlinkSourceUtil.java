package com.ytx.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import com.ytx.constant.Constant;
import com.ytx.function.CustomStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

public class FlinkSourceUtil {
//    获取kafkasource
    public static KafkaSource<String> getKafkaSource(String topic,String groupId){
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CustomStringSchema())
                .build();
        return source;
    }
    public  static MySqlSource getMySqlSource( String databases,String tableName){
        Properties prop = new Properties();
        prop.put("useSSL","false");
        prop.put("decimal.handling.mode","double");
        prop.put("time.precision.mode","connect");
        prop.setProperty("scan.incremental.snapshot.chunk.key-column", "id");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.39.48.36")
                .port(3306)
                .databaseList(databases) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(databases+"."+tableName) // 设置捕获的表
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .startupOptions(StartupOptions.initial())  // 从最早位点启动
//              .startupOptions(StartupOptions.latest()) // 从最晚位点启动
                .debeziumProperties(prop)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
        return mySqlSource;
    }
}
