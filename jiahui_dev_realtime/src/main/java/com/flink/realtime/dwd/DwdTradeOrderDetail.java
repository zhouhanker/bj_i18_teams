package com.flink.realtime.dwd;


import com.flink.realtime.common.base.BaseSQLApp;
import com.flink.realtime.common.constant.Constant;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package flink.realtime.dwd.db.app.DwdTradeOrderDetail
 * @Author guo.jia.hui
 * @Date 2025/4/15 15:09
 * @description: 订单事实表
 */

public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(
                10013,
                4
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 设置状态保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        //TODO 从kafka的topic_db主题中读取数据 创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        //TODO 过滤出订单明细数据
        Table orderDetail = tableEnv.sqlQuery("select " +
                "`after`['id'] id,\n" +
                "`after`['order_id'] order_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "`after`['sku_name'] sku_name,\n" +
                "`after`['create_time'] create_time,\n" +
                "`after`['source_id'] source_id,\n" +
                "`after`['source_type'] source_type,\n" +
                "`after`['sku_num'] sku_num,\n" +
                "`after`['split_total_amount'] split_total_amount," +  // 分摊总金额
                "`after`['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                "`after`['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                "ts_ms " +
                " from topic_db where source['table']='order_detail' and op='c'");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        //TODO 过滤出订单数据
        Table orderInfo = tableEnv.sqlQuery("select " +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['province_id'] province_id " +
                "from topic_db where source['table']='order_info' and op='c'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        //TODO 过滤出明细活动数据
        Table orderDetailActivity = tableEnv.sqlQuery("select " +
                "`after`['order_detail_id'] order_detail_id,\n" +
                "`after`['activity_id'] activity_id,\n" +
                "`after`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db where source['table']='order_detail_activity' and op='c'");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        //TODO 过滤出明细优惠券数据
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "`after`['order_detail_id'] order_detail_id, " +
                        "`after`['coupon_id'] coupon_id " +
                        "from topic_db " +
                        "where source['table']='order_detail_coupon' " +
                        "and op='c'");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        //TODO 关联上述4张表
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
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts_ms " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");

//        result.execute().print();

        //将关联的结果写入到kafka
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
        //写入
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
