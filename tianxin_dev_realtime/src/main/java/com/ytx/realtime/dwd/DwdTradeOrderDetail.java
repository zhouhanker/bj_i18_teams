package com.ytx.realtime.dwd;


import com.ytx.base.BaseSQLApp;
import com.ytx.constant.Constant;
import com.ytx.util.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderDetail extends BaseSQLApp {

    public static void main(String[] args) {

        new DwdTradeOrderDetail().start(10014,4, Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
//    设置状态的保留时间
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
//        从kafka读取数据
        readOdsDb(tableEnv,Constant.TOPIC_DB);
//    过滤出订单明细数据
        Table orderDetail = tableEnv.sqlQuery("select\n" +
                " `after`['id'] id,\n" +
                " `after`['order_id'] order_id,\n" +
                " `after`['sku_id'] sku_id,\n" +
                " `after`['sku_name'] sku_name,\n" +
                " `after`['create_time'] create_time,\n" +
                " `after`['source_id'] source_id,\n" +
                " `after`['source_type'] source_type,\n" +
                " `after`['sku_num'] sku_num,\n" +
                "cast(cast(`after`['sku_num'] as decimal(16,2))*\n" +
                "cast(`after`['order_price'] as decimal(16,2)) as string) split_original_amount,\n" +
                " `after`['split_total_amount'] split_total_amount,\n" +
                " `after`['split_activity_amount'] split_activity_amount,\n" +
                " `after`['split_coupon_amount'] split_coupon_amount,\n" +
                "ts_ms\n" +
                "from topic_db_yue where `source`['table']='order_detail'\n" );
//        orderDetail.execute().print();
       tableEnv.createTemporaryView("order_detail",orderDetail);
//        过滤出订单数据
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                " `after`['id'] id,\n" +
                " `after`['user_id'] user_id,\n" +
                " `after`['province_id'] province_id\n" +
                "from topic_db_yue where  source['table']='order_info'\n" +
                "and op='c'");
//        orderInfo.execute().print();
        tableEnv.createTemporaryView("order_info",orderInfo);

        // 过滤出明细活动数据
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['activity_id'] activity_id, " +
                        "after['activity_rule_id'] activity_rule_id " +
                        "from topic_db_yue " +
                        "where source['table']='order_detail_activity' " +
                        "and op='c'");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
//    过滤出明细优惠数据
        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                " `after`['order_detail_id'] order_detail_id,\n" +
                " `after`['coupon_id'] coupon_id\n" +
                "from topic_db_yue where  source['table']='order_detail_coupon'\n" +
                "and op='c'");
        tableEnv.createTemporaryView("order_detail_coupon",orderDetailCoupon);

//关联4张表
        Table result = tableEnv.sqlQuery(
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
                        "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(od.create_time AS BIGINT), 3), 'yyyy-MM-dd') date_id," +  // 年月日
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

         result.execute().print();
//        tableEnv.sqlQuery("select after['create_time'] from topic_db_yue where source['table']='order_detail'").execute().print();

//            写到kafka
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
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
                        ")" + Sqlutil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

//      result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
