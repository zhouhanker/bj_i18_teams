package com.sdy.dwd;


import com.sdy.bean.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.sdy.retail.v1.realtime.dwd.Dwdpaod
 * @Author danyu-shi
 * @Date 2025/4/13 21:39
 * @description:
 * 下单事实表
 */
public class Dwdpaod {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dwdRf = KafkaUtil.getKafkaSource(env, "stream-dev2-danyushi", "dwd_rf");
//        dwdRf.print();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

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

//        订单表
        Table detailINfo = tableEnv.sqlQuery("select \n" +
                "`after`['id'] as id,\n" +
                "`after`['order_id'] as order_id,\n" +
                "`after`['sku_id'] as sku_id,\n" +
                "`after`['sku_name'] as sku_name,\n" +
                "`after`['create_time'] as create_time,\n" +
                "`source`['source_id'] as source_id,\n" +
                "`source`['source_type'] as source_type,\n" +
                "`after`['sku_num'] as sku_num,\n" +
                "cast(cast(`after`['sku_num'] as decimal(16,2)) * cast(`after`['order_price'] as decimal(16,2)) as String) as split_original_amount,\n" +
                "`after`['split_total_amount'] as split_total_amount,\n" +
                "`after`['split_activity_amount'] as split_activity_amount,\n" +
                "`after`['split_coupon_amount'] as split_coupon_amount,\n" +
                "op,\n" +
                "ts_ms\n" +
                "from topic_db\n" +
                "where `source`['table'] = 'order_detail';");
        tableEnv.createTemporaryView("detailINfo",detailINfo);

//
//        detailINfo.execute().print();

//         定单明细
        Table orderinfo = tableEnv.sqlQuery("select \n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['province_id'] province_id\n" +
                "from topic_db \n" +
                "where `source`['table'] = 'order_info';");
//        orderinfo.execute().print();
        tableEnv.createTemporaryView("orderinfo",orderinfo);

//        订单明细活动

        Table actINfo = tableEnv.sqlQuery("select \n" +
                "`after`['order_detail_id'] order_detail_id,\n" +
                "`after`['activity_id'] activity_id,\n" +
                "`after`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db \n" +
                "where `source`['table'] = 'order_detail_activity'");
//        actINfo.execute().print();


        tableEnv.createTemporaryView("actINfo",actINfo);
////          订单明细优惠券
//
        Table coupInfo = tableEnv.sqlQuery("select \n" +
                "`after`['order_detail_id'] order_detail_id,\n" +
                "`after`['coupon_id'] coupon_id \n" +
                "from topic_db \n" +
                "where `source`['table'] = 'order_detail_coupon';");
//        coupInfo.execute().print();
        tableEnv.createTemporaryView("coupInfo",coupInfo);


        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts_ms " +
                        "from detailINfo od " +
                        "join orderinfo oi on od.order_id=oi.id " +
                        "left join actINfo act " +
                        "on od.id=act.order_detail_id " +
                        "left join coupInfo cou " +
                        "on od.id=cou.order_detail_id ");
        result.execute().print();

//        tableEnv.executeSql("CREATE TABLE stream_DwdXDTable_danyushi(\n" +
//                "id string," +
//                "order_id string," +
//                "user_id string," +
//                "sku_id string," +
//                "sku_name string," +
//                "province_id string," +
//                "activity_id string," +
//                "activity_rule_id string," +
//                "coupon_id string," +
//                "date_id string," +
//                "create_time string," +
//                "sku_num string," +
//                "split_original_amount string," +
//                "split_activity_amount string," +
//                "split_coupon_amount string," +
//                "split_total_amount string," +
//                "ts_ms bigint," +
//                "primary key(id) not enforced " +
//                ")WITH(\n" +
//                "'connector' = 'upsert-kafka',\n" +
//                "'topic' = 'stream_DwdXDTable_danyushi',\n" +
//                "'properties.bootstrap.servers' = 'cdh02:9092',\n" +
//                "'key.format' = 'json',\n" +
//                "'value.format' = 'json'\n" +
//                ");");
//
//
//        result.executeInsert("stream_DwdXDTable_danyushi");


//        env.execute();

    }
}
