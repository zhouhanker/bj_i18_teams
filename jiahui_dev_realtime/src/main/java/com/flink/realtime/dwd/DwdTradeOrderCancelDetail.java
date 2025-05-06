package com.flink.realtime.dwd;


import com.flink.realtime.common.base.BaseSQLApp;
import com.flink.realtime.common.constant.Constant;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package flink.realtime.dwd.db.app.DwdTradeOrderCancelDetail
 * @Author guo.jia.hui
 * @Date 2025/4/15 18:58
 * @description: 取消订单事实表
 */
public class DwdTradeOrderCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(
                10015,
                4
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 设置状态的保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        //TODO 从kafka的topic_db主题中读取数据
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
        //TODO 过滤出取消订单行为
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " `ts_ms` " +
                "from topic_db " +
                "where source['table']='order_info' " +
                "and op='c' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts_ms bigint," +
                        "primary key(id) not enforced " +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = '" + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "',\n" +
                        "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")");

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
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts_ms " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");
        //result.execute().print();

        //TODO 将关联的结果写到kafka主题中
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + "(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "operate_time string," +
                        "sku_num string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts_ms bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = '" + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + "',\n" +
                        "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")");
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
