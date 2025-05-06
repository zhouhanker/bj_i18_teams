package com.common.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic,String groupid){

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setTopics(topic)
                .setGroupId(groupid)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null) {
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
        return source;
    }
    //获取mysqlsource
    public static MySqlSource<String> getMySqlSource( String database ,String tablename){
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","string");
        properties.setProperty("time.precision.mode","connect");


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.160.60.17")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .port(3306)
                .databaseList(database)
                .tableList(database+"."+tablename)
                .username("root")
                .password("Zh1028,./")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
//        {"before":null,"after":{"id":47885,"log":"{\"common\":{\"ar\":\"21\",\"ba\":\"xiaomi\",\"ch\":\"xiaomi\",\"is_new\":\"1\",\"md\":\"xiaomi 12 ultra \",\"mid\":\"mid_306\",\"os\":\"Android 13.0\",\"sid\":\"f8291333-fe2b-44f2-8eb4-6aadc4b29c94\",\"uid\":\"887\",\"vc\":\"v2.1.134\"},\"page\":{\"during_time\":14659,\"item\":\"3,32,6\",\"item_type\":\"sku_ids\",\"last_page_id\":\"cart\",\"page_id\":\"order\"},\"ts\":1745162978677}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"realtime_v1","sequence":null,"table":"z_log","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1745392456345,"transaction":null}


        return mySqlSource;
    }
}
