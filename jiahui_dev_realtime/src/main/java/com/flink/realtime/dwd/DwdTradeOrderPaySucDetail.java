package com.flink.realtime.dwd;

import com.flink.realtime.common.base.BaseSQLApp;
import com.flink.realtime.common.constant.Constant;
import com.flink.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package flink.realtime.db.app.DwdTradeOrderPaySucDetail
 * @Author guo.jia.hui
 * @Date 2025/4/15 20:04
 * @description: 支付成功事实表
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(
                10015,
                4
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从下单事实表读取数据 创建动态表
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
                        "create_time bigint," +
                        "sku_num string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts_ms bigint" +
                        ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        //TODO 从topic_db主题中读取数据  创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
        //TODO 过滤出支付成功数据
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "after['user_id'] user_id," +
                "after['order_id'] order_id," +
                "after['payment_type'] payment_type," +
                "after['callback_time'] callback_time," +
                "ts_ms," +
                "PROCTIME() as pt " +  // 添加处理时间列
                "from topic_db " +
                "where source['table']='payment_info' " +
                "and `op`='u' " +
                "and `after`['payment_status'] is not null " +
                "and `after`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        //tableEnv.executeSql("select * from dwd_trade_order_detail").print();
        //TODO 从HBase中读取字典数据 创建动态表
        readBaseDic(tableEnv);
        //TODO 和字典进行关联---lookup join 和下单数据进行关联---IntervalJoin
        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "   od.id AS order_detail_id, " +
                        "   od.order_id, " +
                        "   od.user_id, " +
                        "   od.sku_id, " +
                        "   od.sku_name, " +
                        "   od.province_id, " +
                        "   od.activity_id, " +
                        "   od.activity_rule_id, " +
                        "   od.coupon_id, " +
                        "   pi.payment_type AS payment_type_code, " +
                        "   dic.dic_name AS payment_type_name, " +
                        "   pi.callback_time, " +
                        "   od.sku_num, " +
                        "   od.split_activity_amount, " +
                        "   od.split_coupon_amount, " +
                        "   od.split_total_amount AS split_payment_amount, " +
                        "   pi.ts_ms " +
                        "FROM payment_info pi " +
                        "JOIN dwd_trade_order_detail od " +
                        "   ON pi.order_id = od.order_id " +
                        "   AND TO_TIMESTAMP(FROM_UNIXTIME(od.ts_ms/1000)) >= " +
                        "       TO_TIMESTAMP(FROM_UNIXTIME(pi.ts_ms/1000)) - INTERVAL '30' MINUTE " +
                        "   AND TO_TIMESTAMP(FROM_UNIXTIME(od.ts_ms/1000)) <= " +
                        "       TO_TIMESTAMP(FROM_UNIXTIME(pi.ts_ms/1000)) + INTERVAL '5' SECOND " +
                        "JOIN base_dic FOR SYSTEM_TIME AS OF pi.pt AS dic " +
                        "   ON pi.payment_type = dic.dic_code " +
                        "   AND dic.dic_code IS NOT NULL"
        );
        //result.execute().print();
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "(" +
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
                        "payment_type_name string," +
                        "callback_time string," +
                        "sku_num string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_payment_amount string," +
                        "ts_ms bigint," +
                        "primary key(order_detail_id) not enforced " +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = '" + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "',\n" +
                        "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")");
        //写入
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}
