package com.sdy.dwd;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.sdy.retail.v1.realtime.dwd.DwdTrade
 * @Author danyu-shi
 * @Date 2025/4/14 20:03
 * @description:
 * 退单
 */
public class DwdTrade {
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

//        tableEnv.executeSql("select * from topic_db").print();

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
//                tableEnv.executeSql("select * from base_dic").print();



        // 2. 过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['user_id'] user_id," +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "`after`['refund_type'] refund_type," +
                        "`after`['refund_num'] refund_num," +
                        "`after`['refund_amount'] refund_amount," +
                        "`after`['refund_reason_type'] refund_reason_type," +
                        "`after`['refund_reason_txt'] refund_reason_txt," +
                        "`after`['create_time'] create_time," +
                        "proc_time," +
                        "op," +
                        "ts_ms " +
                        "from topic_db " +
                        "where `source`['table'] = 'order_refund_info' " +
                        "and op= 'c' ");

//        orderRefundInfo.execute().print();

        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);


        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['province_id'] province_id " +
                        "from topic_db " +
                        "where `source`['table'] ='order_info' " +
                        "and `op`='u'" +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1005' ");


//        orderInfo.execute().print();
        tableEnv.createTemporaryView("order_info", orderInfo);


        Table result = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts_ms " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.proc_time as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.proc_time as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");

        result.execute().print();


//        tableEnv.executeSql("create table stream_dwdTDTable_danyushi(" +
//                "id string," +
//                "user_id string," +
//                "order_id string," +
//                "sku_id string," +
//                "province_id string," +
//                "date_id string," +
//                "create_time string," +
//                "refund_type_code string," +
//                "refund_type_name string," +
//                "refund_reason_type_code string," +
//                "refund_reason_type_name string," +
//                "refund_reason_txt string," +
//                "refund_num string," +
//                "refund_amount string," +
//                "ts bigint " +
//                ")WITH(\n" +
//                "'connector' = 'upsert-kafka',\n" +
//                "'topic' = 'stream_dwdTDTable_danyushi',\n" +
//                "'properties.bootstrap.servers' = 'cdh02:9092',\n" +
//                "'key.format' = 'json',\n" +
//                "'value.format' = 'json'\n" +
//                ");");
//
//
//
//        result.executeInsert("stream_dwdTDTable_danyushi");


    }
}
