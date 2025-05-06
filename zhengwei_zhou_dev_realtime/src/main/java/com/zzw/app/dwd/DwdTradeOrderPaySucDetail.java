package com.zzw.app.dwd;

import com.zzw.constant.Constant;
import com.zzw.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.lzy.app.bwd.DwdTradeOrderPaySucDetail
 * @Author zhengwei_zhou
 * @Date 2025/4/14 11:04
 * @description: DwdTradeOrderPaySucDetail
 */

public class DwdTradeOrderPaySucDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        tableEnv.executeSql("select * from topic_db").print();


        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
//        tableEnv.executeSql("select * from base_dic").print();

        //TODO 从下单事实表读取数据 创建动态表
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        " id string," +
                        " order_id string," +
                        " user_id string," +
                        " sku_id string," +
                        " sku_name string," +
                        " province_id string," +
                        " activity_id string," +
                        " activity_rule_id string," +
                        " coupon_id string," +
                        " date_id string," +
                        " create_time string," +
                        " sku_num string," +
                        " split_original_amount string," +
                        " split_activity_amount string," +
                        " split_coupon_amount string," +
                        " split_total_amount string," +
                        " ts_ms bigint " +
                        " )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));


        //TODO 过滤出支付成功数据
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "after['user_id'] user_id," +
                "after['order_id'] order_id," +
                "after['payment_type'] payment_type," +
                "after['callback_time'] callback_time," +
                "ts_ms " +
                "from topic_db " +
                "where source['table'] ='payment_info' " +
                "and `op`='r' " +
                "and `after`['payment_status'] is not null " +
                "and `after`['payment_status'] = '1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);
//        paymentInfo.execute().print();

        //TODO 和字典进行关联---lookup join 和下单数据进行关联---IntervalJoin
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
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts_ms " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id = od.order_id ");
//        result.execute().print();

        //TODO 将关联的结果写到kafka主题中
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+"(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts_ms bigint ," +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        env.execute("DwdTradeOrderPaySucDetail");
    }
}
