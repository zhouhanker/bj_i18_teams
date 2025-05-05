package com.ytx.realtime.dwd;


import com.ytx.base.BaseSQLApp;
import com.ytx.constant.Constant;
import com.ytx.util.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
/*
交易域退单事务事实表
 */
public class DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10017,4, Constant.TOPIC_DB);

    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
       tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        readOdsDb(tableEnv,Constant.TOPIC_DB);
        readBaseDic(tableEnv);
//过滤退单表数据 order_refund_info
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['refund_type'] refund_type," +
                        "after['refund_num'] refund_num," +
                        "after['refund_amount'] refund_amount," +
                        "after['refund_reason_type'] refund_reason_type," +
                        "after['refund_reason_txt'] refund_reason_txt," +
                        "after['create_time'] create_time," +
                        "pt," +
                        "ts_ms " +
                        "from topic_db_yue " +
                        "where source['table']='order_refund_info' " +
                        "and `op`='c' ");
//        orderRefundInfo.execute().print();
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

  //   过滤订单表中的退单数据: order_info
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['province_id'] province_id," +
                        "`before` " +
                        "from topic_db_yue " +
                        "where source['table']='order_info' " +
                        "and `op`='u'" +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1005' ");
//        orderRefundInfo.execute().print();
        tableEnv.createTemporaryView("order_info", orderInfo);
//        join连表
        Table result = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(ri.create_time AS BIGINT), 3), 'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts_ms " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");
        result.execute().print();
        // 5. 写出到 kafka

      tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts_ms bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + Sqlutil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

//      result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);


    }
}
