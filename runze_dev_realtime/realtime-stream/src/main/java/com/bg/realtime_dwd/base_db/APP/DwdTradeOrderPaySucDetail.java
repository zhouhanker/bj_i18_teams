package com.bg.realtime_dwd.base_db.APP;

import com.bg.common.base.BaseSQLApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.bg.realtime_dwd.base_db.app.DwdTradeOrderPaySucDetail
 * @Author Chen.Run.ze
 * @Date 2025/4/11 15:07
 * @description: 支付成功事实表
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );

    }

    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //TODO 从下单事实表读取数据 创建动态表
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
                        "ts_ms bigint," +
                        "et as to_timestamp_ltz(ts_ms, 0), " +
                        "watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        //TODO 从topic_db主题中读取数据  创建动态表
        readOdsDb(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
        //TODO 过滤出支付成功数据
        Table paymentInfo = tEnv.sqlQuery("select " +
                "after['user_id'] user_id," +
                "after['order_id'] order_id," +
                "after['payment_type'] payment_type," +
                "after['callback_time'] callback_time," +
                "`pt`," +
                "ts_ms, " +
                "et " +
                "from topic_db " +
                "where `source`['table']='payment_info' " +
                "and `op`='u' " +
                "and `before`['payment_status'] is not null " +
                "and `after`['payment_status']='1602' ");
        tEnv.createTemporaryView("payment_info", paymentInfo);

        //TODO 从HBase中读取字典数据 创建动态表
        readBaseDic(tEnv);
        //TODO 和字典进行关联---lookup join 和下单数据进行关联---IntervalJoin
        Table result = tEnv.sqlQuery(
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
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");

        //TODO 将关联的结果写到kafka主题中
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+"(" +
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
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        //TODO 写入
        //+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+
        //| op |                order_detail_id |                       order_id |                        user_id |                         sku_id |                       sku_name |                    province_id |                    activity_id |               activity_rule_id |                      coupon_id |              payment_type_code |              payment_type_name |                  callback_time |                        sku_num |          split_original_amount |          split_activity_amount |            split_coupon_amount |           split_payment_amount |                ts_ms |
        //+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+
        //| +I |                           3444 |                           2427 |                            633 |                             30 |  CAREMiLLE珂曼奶油小方口红 ... |                              9 |                         <NULL> |                         <NULL> |                         <NULL> |                           1102 |                           微信 |                  1745852856000 |                              1 |                        69.0000 |                            0.0 |                            0.0 |                           69.0 |        1745824056277 |
        //| +I |                           3447 |                           2432 |                            459 |                             24 |  金沙河面条 原味银丝挂面 龙... |                              5 |                         <NULL> |                         <NULL> |                         <NULL> |                           1103 |                           银联 |                  1745852856000 |                              1 |                        11.0000 |                            0.0 |                            0.0 |                           11.0 |        1745824056470 |
        //| +I |                           3448 |                           2433 |                            638 |                              6 |  Redmi 10X 4G Helio G85游戏... |                             27 |                         <NULL> |                         <NULL> |                         <NULL> |                           1102 |                           微信 |                  1745852856000 |                              1 |                      1299.0000 |                            0.0 |                            0.0 |                         1299.0 |        1745824056568 |
        //| +I |                           3453 |                           2438 |                            640 |                             14 | 联想（Lenovo） 拯救者Y9000P... |                              5 |                              3 |                              4 |                         <NULL> |                           1102 |                           微信 |                  1745852856000 |                              1 |                     11999.0000 |                          250.0 |                            0.0 |                        11749.0 |        1745824056684 |
        //| +I |                           3450 |                           2435 |                            634 |                              8 | Apple iPhone 12 (A2404) 64G... |                             31 |                         <NULL> |                         <NULL> |                         <NULL> |                           1102 |                           微信 |                  1745852856000 |                              1 |                      8197.0000 |                            0.0 |                            0.0 |                         8197.0 |        1745824056617 |
        result.execute().print();
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

    }
}
