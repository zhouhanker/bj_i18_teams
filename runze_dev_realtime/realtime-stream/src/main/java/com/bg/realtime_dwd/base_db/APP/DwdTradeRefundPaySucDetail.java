package com.bg.realtime_dwd.base_db.APP;

import com.bg.common.base.BaseSQLApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.bg.realtime_dwd.base_db.app.DwdTradeRefundPaySucDetail
 * @Author Chen.Run.ze
 * @Date 2025/4/11 15:51
 * @description: 退款成功事实表
 */
public class DwdTradeRefundPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10018, 4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS
        );
    }

    @Override
    public void handle(StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 1. 读取 topic_db
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        // 2. 读取 字典表
        readBaseDic(tEnv);

        // 3. 过滤退款成功表数据
        Table refundPayment = tEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['payment_type'] payment_type," +
                        "after['callback_time'] callback_time," +
                        "after['total_amount'] total_amount," +
                        "pt, " +
                        "ts_ms " +
                        "from topic_db " +
                        "where `source`['table']='refund_payment' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='1602'");
        tEnv.createTemporaryView("refund_payment", refundPayment);

        // 4. 过滤退单表中的退单成功的数据
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['refund_num'] refund_num " +
                        "from topic_db " +
                        "where `source`['table']='order_refund_info' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='0705'");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 5. 过滤订单表中的退款成功的数据
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from topic_db " +
                        "where `source`['table']='order_info' " +
                        "and `op`='u' " +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1006'");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 6. 4 张表的 join
        Table result = tEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(rp.callback_time AS BIGINT), 3), 'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts_ms " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " +
                        "on rp.payment_type=dic.dic_code ");

        // 7.写出到 kafka
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+"(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts_ms bigint ," +
                "PRIMARY KEY (id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));

        //TODO 写入
        //+----+----+---------+----------+--------+-------------+---------------+------------------+--------------+-----------------+-------------+----------------+-----------------+
        //| op | id | user_id | order_id | sku_id | province_id |  payment_type |payment_type_name |      date_id |   callback_time |  refund_num |   total_amount |           ts_ms |
        //+----+----+---------+----------+--------+-------------+---------------+------------------+--------------+-----------------+-------------+----------------+-----------------+
        //| +I | 87 |     215 |     2511 |      3 |          31 |          1101 |            支付宝 |    2025-04-28 |   1745852860000 |           1 |         6499.0 |   1745824060565 |
        //| +I | 90 |     684 |     2582 |     22 |          24 |          1101 |            支付宝 |    2025-04-28 |   1745852864000 |           1 |           39.0 |   1745824064517 |
        //| +I | 86 |     364 |     2476 |     31 |          22 |          1101 |            支付宝 |    2025-04-28 |   1745852859000 |           1 |           69.0 |   1745824058926 |
        //| +I | 88 |     658 |     2559 |     26 |           4 |          1101 |            支付宝 |    2025-04-28 |   1745852863000 |           1 |          129.0 |   1745824063420 |
        //| +I | 89 |     682 |     2574 |      3 |          18 |          1101 |            支付宝 |    2025-04-28 |   1745852864000 |           1 |         6499.0 |   1745824064303 |
        result.execute().print();
//        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);


    }


}