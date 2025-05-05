package util;

import constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Properties;

public class FlinkSourceUtil {
  public static KafkaSource<String> getKafkaSource(String topic,String groupId){
      KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
              .setBootstrapServers(Constant.KAFKA_BROKERS)
              .setTopics(topic)
              .setGroupId(groupId)
//在生产环境中，一般为了保证消费的精准一次性，需要手动维护偏移量，KafkaSource->KafkaSourceReader->存储偏移量
              .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
              //从最末尾位点开始消费
              .setStartingOffsets(OffsetsInitializer.latest())

              .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                  @Override
                  public String deserialize(byte[] bytes) throws IOException {
                      if (bytes!=null){
                          return new String(bytes);
                      }
                      return "";
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

    public static MySqlSource<String> getMysqlSourceUtil(String database,String tableName){
        Properties props = new Properties();
        props.setProperty("useSsL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .startupOptions(StartupOptions.initial()) // 从最早位点启动
                .port(Constant.MYSQL_PORT)
                .databaseList(database)
                .tableList(database+"."+tableName)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        return mySqlSource;
    }
}
