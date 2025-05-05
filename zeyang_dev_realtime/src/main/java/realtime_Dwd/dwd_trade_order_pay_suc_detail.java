package realtime_Dwd;

import Base.BasesqlApp;
import constat.constat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.Sqlutil;

/**
 * @Package realtime_Dwd.dwd_trade_order_pay_suc_detail
 * @Author ayang
 * @Date 2025/4/14 10:37
 * @description: 支付成功事实表
 */
//已经跑了

public class dwd_trade_order_pay_suc_detail extends BasesqlApp {
    public static void main(String[] args) {
        new dwd_trade_order_pay_suc_detail().start(10007,4, constat.
                TOPIC_Dwd_Trade_Order_PaySuc_Detail);

    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
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
                        "et as to_timestamp_ltz(ts_ms, 3), " +
                        "watermark for et as et - interval '3' second " +
                        ")" + Sqlutil.getKafkaDDL(constat.TOPIC_DWD_TRADE_ORDER_DETAIL, constat.TOPIC_DWD_TRADE_ORDER_DETAIL));

// 2. 读取 topic_db
        readOdsDb(tableEnv, constat.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

// 3. 读取 字典表
        readBaseDic(tableEnv);
        // 3. 过滤退款成功表数据
        Table payment_info = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['payment_type'] payment_type," +
                        "after['callback_time'] callback_time," +
                        "after['total_amount'] total_amount," +
                        "proc_time, " +
                        "ts_ms ," +
                        "et  "+
                        "from topic_table_v1 " +
                        "where source['table']='refund_payment' " +
                        "and `op`='u' " +
                        "and before['refund_status'] is not null " +
                        "and `after`['refund_status']='1602'");
        tableEnv.createTemporaryView("payment_info", payment_info);
//        refundPayment.execute().print();

        // 4. 过滤退单表中的退单成功的数据
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['refund_num'] refund_num " +
                        "from topic_table_v1 " +
                        "where source['table']='order_refund_info' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
//        orderRefundInfo.execute().print();

        // 5. 过滤订单表中的退款成功的数据
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from topic_table_v1 " +
                        "where source['table']='order_info' " +
                        "and `op`='u' " +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();
        // 6. 4 张表的 join
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
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts_ms " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
//                        "and od.et >= pi.et - interval '30' minute " +
//                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.proc_time as dic " +
                        "on pi.payment_type=dic.dic_code ");
        result.execute().print();

        // 6. 写出到 kafka 中
        tableEnv.executeSql("create table dwd_trade_order_payment_success(" +
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
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts_ms bigint ," +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED " +

                ")" + Sqlutil.getUpsertKafkaDDL(constat.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
//        result.executeInsert(constat.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}
