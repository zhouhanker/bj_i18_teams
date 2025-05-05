package com.bg.common.util;

import com.bg.common.constant.Constant;

/**
 * @Package com.bg.common.util.SQLUtil
 * @Author Chen.Run.ze
 * @Date 2025/4/10 22:38
 * @description: FlinkSQL操作工作类
 */
public class SQLUtil {
    //获取Kafka连接属性
    public static String getKafkaDDL(String topic,String groupId){
        return "WITH (\n" +
               "  'connector' = 'kafka',\n" +
               "  'topic' = '"+ topic +"',\n" +
               "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS+"',\n" +
               "  'properties.group.id' = '"+ groupId +"',\n" +
               "  'scan.startup.mode' = 'earliest-offset',\n" +
               "  'format' = 'json'\n" +
               ")";
    }

    //获取HBase连接属性
    public static String getHBaseDDL(String tableName){
        return  "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '"+ tableName +"',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181'\n" +
                ")";
    }

    //获取upsert-kafka连接属性
    public static String getUpsertKafkaDDL(String topic){
        return  "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ topic +"',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
