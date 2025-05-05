package com.jl.dwd;



import com.jl.constant.Constant;
import com.jl.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.jl.DwdTradeRefundPaySucDetail
 * @Author jia.le
 * @Date 2025/4/14 11:46
 * @description: DwdTradeRefundPaySucDetail
 */

public class DwdTradeRefundPaySucDetail {
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

        // 3. 过滤退款成功表数据
        Table refundPayment = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +
                        " after['order_id'] order_id," +
                        " after['sku_id'] sku_id," +
                        " after['payment_type'] payment_type," +
                        " after['callback_time'] callback_time," +
                        " after['total_amount'] total_amount," +
                        " ts_ms " +
                        " from topic_db " +
                        " where source['table']='refund_payment' " +
                        " and `op`='r' " +
                        " and `after`['refund_status'] is not null " +
                        " and `after`['refund_status']='1602'");
        tableEnv.createTemporaryView("refund_payment", refundPayment);
//        refundPayment.execute().print();

        // 4. 过滤退单表中的退单成功的数据
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        " after['order_id'] order_id," +
                        " after['sku_id'] sku_id," +
                        " after['refund_num'] refund_num " +
                        " from topic_db " +
                        " where source['table']='order_refund_info' " +
                        " and `op`='r' " +
                        " and `after`['refund_status'] is not null " +
                        " and `after`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
//        orderRefundInfo.execute().print();

        // 5. 过滤订单表中的退款成功的数据
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from topic_db " +
                        "where source['table']='order_info' " +
                        "and `op`='r' " +
                        "and `after`['order_status'] is not null " +
                        "and `after`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();

        // 6. 4 张表的 join
        Table result = tableEnv.sqlQuery(
                "select " +
                        " rp.id," +
                        " oi.user_id," +
                        " rp.order_id," +
                        " rp.sku_id," +
                        " oi.province_id," +
                        " rp.payment_type," +
                        " date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(rp.callback_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        " rp.callback_time," +
                        " ori.refund_num," +
                        " rp.total_amount," +
                        " rp.ts_ms " +
                        " from refund_payment rp " +
                        " join order_refund_info ori " +
                        " on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        " join order_info oi " +
                        " on rp.order_id=oi.id ");
//        result.execute().print();

        // 7.写出到 kafka
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+"(" +
                " id string," +
                " user_id string," +
                " order_id string," +
                " sku_id string," +
                " province_id string," +

                " payment_type_code string," +
                " date_id string," +
                " callback_time string," +
                " refund_num string," +
                " refund_amount string," +
                " ts_ms bigint ," +
                " PRIMARY KEY (id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        
        env.execute("DwdTradeRefundPaySucDetail");


    }
}
