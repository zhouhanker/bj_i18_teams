package com.jl.utils;



import com.jl.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;


public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> build = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) {
                                if (message != null) {
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        return build;
    }

    //获取MySqlSource
    public static MySqlSource<String> getMySqlSource(String database,String tableName){
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "double");
        properties.setProperty("time.precision.mode","connect");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.earliest())
                .databaseList(database)
                .tableList(database + "." + tableName)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        return mySqlSource;
    }
}
