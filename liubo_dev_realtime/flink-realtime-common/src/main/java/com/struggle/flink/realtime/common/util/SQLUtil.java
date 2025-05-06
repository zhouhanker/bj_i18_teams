package com.struggle.flink.realtime.common.util;

import com.struggle.flink.realtime.common.constant.Constant;

/**
 * @ version 1.0
 * @ Package com.struggle.flink.realtime.common.util.SQLUtil
 * @ Author liu.bo
 * @ Date 2025/5/3 14:24
 * @ description:sql工具类
 */
public class SQLUtil {
    public static String getKafkaDDL(String topic, String groupId) {
        //获取kafka连接器的连接属性
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getHBaseDDL(String tableName) {
        return "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    //读取upsert-kafka连接器的连接属性
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
