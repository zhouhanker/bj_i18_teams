package com.Realtime_dwd;

import com.Base.BasesqlApp;
import com.Constat.constat;
import com.utils.Sqlutil;
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
    public void handle(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        readOdsDb(tableEnv,constat.TOPIC_DWD_TRADE_ORDER_CANCEL);
        //从下单事实表中获取数据
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
                        "ts bigint " +
                        ")" + Sqlutil.getKafkaDDL(constat.TOPIC_DWD_TRADE_ORDER_DETAIL, constat.TOPIC_DWD_TRADE_ORDER_DETAIL));

//        tableEnv.executeSql("select * from dwd_trade_order_detail").print();
        // 3. 从 topic_db 过滤出订单取消数据
        Table orderCancel = tableEnv.sqlQuery("select " +
                "`after`['id'] id," +
                "`after`['operate_time'] operate_time," +
                " `ts_ms` " +
                "from topic_table_v1 " +
                "where " +
                "source['table']='order_info' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);
        // 4. 订单取消表和下单表进行 join
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
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
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
//        result.execute().print();
        //订单取消表和下单表发送到kafka
        tableEnv.executeSql(
                "create table dwd_trade_order_cancel_detail(" +
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
                        "primary key(id) not enforced " +
                        ")" + Sqlutil.getUpsertKafkaDDL(constat.TOPIC_DWD_TRADE_ORDER_CANCEL));
        result.executeInsert(constat.TOPIC_DWD_TRADE_ORDER_CANCEL);

    }
}
