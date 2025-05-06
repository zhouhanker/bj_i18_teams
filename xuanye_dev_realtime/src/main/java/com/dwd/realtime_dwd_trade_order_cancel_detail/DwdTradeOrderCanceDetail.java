package com.dwd.realtime_dwd_trade_order_cancel_detail;

import com.common.base.BaseSQLApp;
import com.common.constant.Constant;
import com.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderCanceDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderCanceDetail().start(10015,4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //todo 设置状态的保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        //TODO 从kafka的topic_db主题中读取数据
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
        //TODO 过滤出取消订单行为
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " `ts_ms` ts " +
                " from KafkaTable " +
                " where `source`['table'] = 'order_info' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);
//        orderCancel.execute().print();
        //TODO 从下单事实表中获取下单数据
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
                        "ts_ms  bigint " +
                        ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        //TODO 将下单事实表和取消订单表进行关联
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
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");
//        tableEnv.createTemporaryView("result", result);
//        result.execute().print();
        //TODO 将关联的结果写到kafka主题中
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
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
                        "ts string ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
