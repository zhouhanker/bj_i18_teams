package com.cj.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cj.realtime.dwd.DwdTradeOrderCancelDetail
 * @Author chen.jian
 * @Date 2025/4/11 13:36
 * @description: 取消订单事实表
 */
public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//        从kafka的topic_db主题中读取数据
        tenv.executeSql("CREATE TABLE db (\n" +
                "  before MAP<string,string>,\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  proc_time  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        过滤出取消订单行为
        Table orderCancel = tenv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                "  ts_ms as ts " +
                "from db " +
                "where source['table']='order_info' " +
                "and `op`='u' " +
                "and `before`['order_status']='1001' " +
                "and `after`['order_status']='1003' ");
//        tenv.toChangelogStream(orderCancel).print();

        tenv.createTemporaryView("order_cancel", orderCancel);

//        从下单事实表中获取下单数据
        tenv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
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
                "ts bigint" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_trade_order_detail',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

//        将下单事实表和取消订单表进行关联
        Table result = tenv.sqlQuery(
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
                        "date_format(FROM_UNIXTIME(CAST(oc.operate_time AS BIGINT) / 1000), 'yyyy-MM-dd') AS order_cancel_date_id," +
                        "date_format(FROM_UNIXTIME(CAST(oc.operate_time AS BIGINT) / 1000), 'yyyy-MM-dd hh:mm:ss') AS operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");
        tenv.toChangelogStream(result).print();

//        将关联的结果写到kafka主题中
        tenv.executeSql(
                "create table dwd_trade_order_cancel(" +
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
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'dwd_trade_order_cancel',\n" +
                        "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ");");
        result.executeInsert("dwd_trade_order_cancel");

        env.execute();
    }
}

