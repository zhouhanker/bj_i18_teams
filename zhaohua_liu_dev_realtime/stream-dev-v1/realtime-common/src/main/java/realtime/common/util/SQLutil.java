package realtime.common.util;

import realtime.common.constant.Constant;

/**
 * @Package realtime.common.util.SQLutil
 * @Author zhaohua.liu
 * @Date 2025/4/9.9:26
 * @description: flinksql工具类
 */
public class SQLutil {
    //todo kafka连接属性
    public static String getKafkaDDL(String topic,String groupID){
        return "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+topic+"',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092'," +
                "    'properties.group.id' = '"+groupID+"'," +
                //latest
//                "    'scan.startup.mode' = 'earliest-offset'," +
                "    'scan.startup.mode' = 'group-offsets'," +
                "    'format' = 'json'" +
                ")";
    }

    //todo hbase连接器属性
    public static String getHbaseDDL(String tableName){
        return " WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + Constant.HBASE_NAMESPACE + ":" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    //todo 获取upsert-kafka连接器的连接属性
    //在传统的 Kafka 数据处理中，消息通常是追加写入的，不支持更新操作。
    // 而 Upsert Kafka 连接器打破了这一限制，它引入了 “Upsert”（插入或更新）的概念，
    // 允许 Flink 应用程序以主键为依据，对 Kafka 主题中的数据进行插入或更新操作。
    //upsert-kafka 连接器支持以 upsert（插入或更新）的模式与 Kafka 进行交互，
    // 它会根据主键来区分是插入新记录还是更新已有记录。
    public static String getUpsertKafkaDDl(String topic){
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n"+
                "  'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");";
    }

}
