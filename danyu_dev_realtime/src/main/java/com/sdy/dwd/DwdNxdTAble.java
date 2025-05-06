package com.sdy.dwd;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.sdy.retail.v1.realtime.dwd.DwdNxdTAble
 * @Author danyu-shi
 * @Date 2025/4/14 13:57
 * @description:
 */
public class DwdNxdTAble {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "    `before` MAP<STRING, STRING>,\n" +
                "    `after` MAP<STRING, STRING>,\n" +
                "    `source` MAP<STRING, STRING>,\n" +
                "    `op` STRING,\n" +
                "    `ts_ms` BIGINT,\n" +
                "    proc_time AS proctime()\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'stream-dev2-danyushi',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

//                tableEnv.executeSql("select * from topic_db").print();

        tableEnv.executeSql("CREATE TABLE DwdXDTable (\n" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "create_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts_ms bigint " +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'stream_DwdXDTable_danyushi',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

// tableEnv.executeSql("select * from DwdXDTable").print();


        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " `op`," +
                " `ts_ms` " +
                "from topic_db " +
                "where `source`['table']='order_info' " +
                "and `op`='u' " +
                "and `before`['order_status']='1001' " +
                "and `after`['order_status']='1003' ");


        tableEnv.createTemporaryView("orderCancel",orderCancel);

//        orderCancel.execute().print();



        Table result = tableEnv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') as order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts_ms " +
                        "from DwdXDTable od " +
                        "join orderCancel oc " +
                        "on od.order_id=oc.id ");

//        result.execute().print();


//                tableEnv.executeSql(
//                        "create table stream_DwdNXDTable_danyushi(" +
//                                "id string," +
//                                "order_id string," +
//                                "user_id string," +
//                                "sku_id string," +
//                                "sku_name string," +
//                                "province_id string," +
//                                "activity_id string," +
//                                "activity_rule_id string," +
//                                "coupon_id string," +
//                                "date_id string," +
//                                "cancel_time string," +
//                                "sku_num string," +
//                                "split_original_amount string," +
//                                "split_activity_amount string," +
//                                "split_coupon_amount string," +
//                                "split_total_amount string," +
//                                "ts_ms BIGINT " +
//                "primary key(id) not enforced " +
//                ")WITH(\n" +
//                "'connector' = 'upsert-kafka',\n" +
//                "'topic' = 'stream_DwdNXDTable_danyushi',\n" +
//                "'properties.bootstrap.servers' = 'cdh02:9092',\n" +
//                "'key.format' = 'json',\n" +
//                "'value.format' = 'json'\n" +
//                ");");
//
//
//
//        result.executeInsert("stream_DwdNXDTable_danyushi");


    }
}
