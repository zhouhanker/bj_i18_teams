package com.gy.realtime_dwd;

import com.gy.Base.BasesqlApp;
import com.gy.constat.constat;
import com.gy.utils.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @Package realtime_Dwd.DwdTradeOrderDetail
 * @Author guangyi_zhou
 * @Date 2025/4/11 15:36
 * @description: 下单事实表
 */

public class DwdTradeOrderDetail extends BasesqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10004, 4,
                "DwdTradeOrderDetail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv, constat.TOPIC_DWD_TRADE_ORDER_DETAIL);

        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['sku_name'] sku_name," +
                        "after['create_time'] create_time," +
                        "after['source_id'] source_id," +
                        "after['source_type'] source_type," +
                        "after['sku_num'] sku_num," +
                        "cast(cast(after['sku_num'] as decimal(16, 2)) * " +
                        "   cast(after['order_price'] as decimal(16, 2)) as String) split_original_amount," + // 分摊原始总金额
                        "after['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "after['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "after['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts_ms " + // 确保最后一列后没有多余逗号
                        "from topic_table_v1 " +
                        "where source['table'] = 'order_detail' " +
                        "and `op` = 'r' and ts_ms is not null ");
        tableEnv.createTemporaryView("order_detail", orderDetail);
//        orderDetail.execute().print();
//
        // 3. 过滤出 oder_info 数据: c
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from topic_table_v1 " +
                        "where " +
                        " source['table']='order_info' " +
                        "and `op`='r' ");
        tableEnv.createTemporaryView("order_info", orderInfo);


//        orderInfo.execute().print();
        // 4. 过滤order_detail_activity 表: insert
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['activity_id'] activity_id, " +
                        "after['activity_rule_id'] activity_rule_id " +
                        "from topic_table_v1 " +
                        "where  " +
                        " source['table']='order_detail_activity' " +
                        "and `op`='r' ");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
//        orderDetailActivity.execute().print();
        // 5. 过滤order_detail_coupon 表: insert
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['coupon_id'] coupon_id " +
                        "from topic_table_v1 " +
                        "where  " +
                        " source['table']='order_detail_coupon' " +
                        "and `op`='r' ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
//        orderDetailCoupon.execute().print();


        Table result_V1 = tableEnv.sqlQuery(
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
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
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
        tableEnv.createTemporaryView("result_V1", result_V1);
//        tableEnv.sqlQuery("Select * from result_V1").execute().print();

        //  7. 写出到 kafka 中
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
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
                        "primary key(id) not enforced " +
                        ")" + Sqlutil.getUpsertKafkaDDL(constat.TOPIC_DWD_TRADE_ORDER_DETAIL));

        result_V1.executeInsert(constat.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

}
