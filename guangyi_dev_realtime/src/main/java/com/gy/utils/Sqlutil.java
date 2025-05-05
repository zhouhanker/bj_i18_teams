package com.gy.utils;

import com.gy.constat.constat;

/**
 * @Package realtime_Dwd.util.Sqlutil
 * @Author guangyi_zhou
 * @Date 2025/4/11 11:15
 * @description: sql工具类
 */
public class Sqlutil {
    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    //获取Hbase连接器的连接属性
    public static String getHBaseDDL(String tableName) {
        return " WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + constat.HBASE_NAMESPACE + ":" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'cdh02:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    //获取upsert-kafka连接器的连接属性
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + constat.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
