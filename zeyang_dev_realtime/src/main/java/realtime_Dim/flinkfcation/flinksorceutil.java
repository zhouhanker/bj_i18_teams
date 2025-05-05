package realtime_Dim.flinkfcation;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import constat.constat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;
/**
 * @Package realtime_Dim.utils.Flink_source
 * @Author a_yang
 * @Date 2025/4/10 9:31
 * @description: source数据类
 */
public class flinksorceutil {
    public static KafkaSource<String> getkafkasorce(String topic){
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(constat.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId("dim_app")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes)   {
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
    public static MySqlSource<String> getmysqlsource(String database , String table){
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "string");
        properties.setProperty("time.precision.mode", "connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(constat.MYSQL_HOST)
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .port(constat.MYSQL_PORT)
                .databaseList()
                .tableList(database +"."+table)
                .username(constat.MYSQL_USER_NAME)
                .password(constat.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        return mySqlSource;

    }
}
