package com.gy.realtime_dwd;

import com.gy.Base.BasesqlApp;
import com.gy.constat.constat;
import com.gy.utils.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
/**
 * @Package realtime_Dwd.DwdTradeOrderCancelDetail
 * @Author ayang
 * @Date 2025/4/11 18:46
 * @description: 取消订单事实表
 */
//已经跑了
public class DwdTradeOrderCancelDetail extends BasesqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10005,4, constat.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //TODO 设置状态的保留时间
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        //TODO 从kafka的topic_db主题中读取数据
        readOdsDb(tEnv, constat.TOPIC_DWD_TRADE_ORDER_CANCEL);
        //TODO 过滤出取消订单行为
        Table orderCancel = tEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " `ts_ms` " +
                "from topic_table_v1 " +
                "where `source`['table']='order_info' " +
                "and `op`='u' " +
                "and `before`['order_status']='1001' " +
                "and `after`['order_status']='1003' ");
        tEnv.createTemporaryView("order_cancel", orderCancel);
        //+----+------+---------------+--------------+
        //| op |   id |  operate_time |        ts_ms |
        //+----+------+---------------+--------------+
        //| +I | 2425 | 1745852856000 |1745824056168 |
        //| +I | 2434 | 1745852856000 |1745824056545 |
        //| +I | 2449 | 1745852857000 |1745824057088 |
//        orderCancel.execute().print();
        //TODO 从下单事实表中获取下单数据
        tEnv.executeSql(
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
                        "ts_ms bigint " +
                        ")" + Sqlutil.getKafkaDDL(constat.TOPIC_DWD_TRADE_ORDER_DETAIL,constat.TOPIC_DWD_TRADE_ORDER_CANCEL));

        //TODO 将下单事实表和取消订单表进行关联
        Table result = tEnv.sqlQuery(
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
                        "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(oc.operate_time AS BIGINT), 3), 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts_ms " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");

        //TODO 将关联的结果写到kafka主题中
        tEnv.executeSql(
                "create table "+constat.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
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
                        "ts_ms bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + Sqlutil.getUpsertKafkaDDL(constat.TOPIC_DWD_TRADE_ORDER_CANCEL));
//        result.execute().print();
        result.executeInsert(constat.TOPIC_DWD_TRADE_ORDER_CANCEL);

    }
}