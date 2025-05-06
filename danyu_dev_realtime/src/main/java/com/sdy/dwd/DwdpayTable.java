package com.sdy.dwd;


import com.sdy.bean.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.sdy.retail.v1.realtime.dwd.DwdpayTable
 * @Author danyu-shi
 * @Date 2025/4/14 15:05
 * @description:
 * 支付事实表
 */
public class DwdpayTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> dwdRf = KafkaUtil.getKafkaSource(env, "stream-dev2-danyushi", "dwd_rf");
        dwdRf.print();
////
//        env.execute();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "    `before` MAP<STRING, STRING>,\n" +
                "    `after` MAP<STRING, STRING>,\n" +
                "    `source` MAP<STRING, STRING>,\n" +
                "    `op` STRING,\n" +
                "    `ts_ms` BIGINT,\n" +
                "    proc_time AS proctime(),\n" +
                "  et as to_timestamp_ltz(ts_ms, 3), " +
                "  watermark for et as et - interval '3' second " +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'stream-dev2-danyushi',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

                tableEnv.executeSql("select * from topic_db").print();

        //下单表
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
                "ts_ms bigint," +
                "et as to_timestamp_ltz(ts_ms, 3), " +
                "watermark for et as et - interval '3' second " +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'stream_DwdXDTable_danyushi',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("select * from DwdXDTable").print();

        //字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "dic_code string,\n" +
                "info Row<dic_name string>,\n" +
                "PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector'='hbase-2.2',\n" +
                "'lookup.partial-cache.max-rows'='500',\n" +
                "'lookup.async'='true',\n" +
                "'lookup.cache'='PARTIAL',\n" +
                "'lookup.partial-cache.expire-after-access'='1 hour',\n" +
                "'lookup.partial-cache.expire-after-write'='1 hour',\n" +
                "'table-name'='ns_danyu_shi:dim_base_dic',\n" +
                "'zookeeper.quorum'='cdh02:2181'\n" +
                ");");
                tableEnv.executeSql("select * from base_dic").print();

        // 4. 从 topic_db 中过滤 payment_info
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "`after`['user_id'] user_id," +
                "`after`['order_id'] order_id," +
                "`after`['payment_type'] payment_type," +
                "`after`['callback_time'] callback_time," +
                "`after`['create_time'] create_time, " +
                "op," +
                "et," +
                "ts_ms " +
                "from topic_db " +
                "where `source`['table'] = 'payment_info' " +
                "and `op`='u' " +
                "and `before`['payment_status'] is not null " +
                "and `after`['payment_status']='1602';");
        paymentInfo.execute().print();

        tableEnv.createTemporaryView("payment_info", paymentInfo);

        Table result = tableEnv.sqlQuery(
                          "select " +
                             "od.id order_detail_id," +
                             "od.order_id," +
                             "od.user_id," +
                             "od.sku_id," +
                             "od.sku_name," +
                             "od.province_id," +
                             "od.activity_id," +
                             "od.activity_rule_id," +
                             "od.coupon_id," +
                             "pi.payment_type payment_type_code ," +
                             "dic.dic_name payment_type_name," +
                             "pi.callback_time," +
                             "od.sku_num," +
                             "od.split_original_amount," +
                             "od.split_activity_amount," +
                             "od.split_coupon_amount," +
                             "od.split_total_amount split_payment_amount," +
                             "pi.ts_ms " +
                             "from payment_info pi " +
                             "join DwdXDTable od " +
                             "on pi.order_id=od.order_id " +
                             "and od.et >= pi.et - interval '30' minute " +
                             "and od.et <= pi.et + interval '5' second " +
                             "join base_dic for system_time as of pi.et as dic " +
                             "on pi.payment_type=dic.dic_code ");
        result.execute().print();

//        tableEnv.executeSql("create table stream_dwdpayTable_danyushi(" +
//                "order_detail_id string," +
//                "order_id string," +
//                "user_id string," +
//                "sku_id string," +
//                "sku_name string," +
//                "province_id string," +
//                "activity_id string," +
//                "activity_rule_id string," +
//                "coupon_id string," +
//                "payment_type_code string," +
//                "payment_type_name string," +
//                "callback_time string," +
//                "sku_num string," +
//                "split_original_amount string," +
//                "split_activity_amount string," +
//                "split_coupon_amount string," +
//                "split_payment_amount string," +
//                "ts bigint " +
//                ")WITH(\n" +
//                        "'connector' = 'upsert-kafka',\n" +
//                        "'topic' = 'stream_dwdpayTable_danyushi',\n" +
//                        "'properties.bootstrap.servers' = 'cdh02:9092',\n" +
//                        "'key.format' = 'json',\n" +
//                        "'value.format' = 'json'\n" +
//                        ");");
//
//        result.executeInsert("stream_dwdpayTable_danyushi");

    }
}
