package realtime.common.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import realtime.common.base.BaseApp;
import realtime.common.constant.Constant;


import java.io.IOException;
import java.util.Properties;

/**
 * @Package realtime.common.util.FlinkSourceUtil
 * @Author zhaohua.liu
 * @Date 2025/4/9.14:08
 * @description: flink获取source的工具类
 */
public class FlinkSourceUtil {


    //todo 获取KafkaSource
    public static KafkaSource<String> getKafkaSource(String topic,String groupId){
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                //从偏移量__consumer_offsets 开始读取,如果没有偏移量就从最早开始
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                //从最早开始读取
//                .setStartingOffsets(OffsetsInitializer.earliest())
                //自定义反序列化器,字节数组反序列化为 String 类型,流不会结束,返回反序列化后数据的类型
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
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
        return kafkaSource;
    }



    //todo CDC获取MysqlSource
    public static MySqlSource<String> getMysqlSource(String database,String tableName){
        Properties properties1 = new Properties();
        //禁用ssl协议
        properties1.setProperty("useSSL", "false");
        Properties properties2 = new Properties();
        //decimal转double
        properties2.setProperty("decimal.handling.mode","double");
        //微秒转毫秒
        properties2.setProperty("time.precision.mode","connect");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(database)
                .tableList(database + "." + tableName)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .jdbcProperties(properties1)
                .debeziumProperties(properties2)
                .build();
        return mySqlSource;
    }

}
