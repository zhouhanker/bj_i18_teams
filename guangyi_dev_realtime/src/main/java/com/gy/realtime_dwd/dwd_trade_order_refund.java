package com.gy.realtime_dwd;


import com.gy.Base.BasesqlApp;
import com.gy.constat.constat;
import com.gy.utils.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package realtime_Dwd.dwd_trade_order_refund
 * @Author ayang
 * @Date 2025/4/14 9:30
 * @description: 退款成功
 */
//已经跑了

public class dwd_trade_order_refund extends BasesqlApp {
    public static void main(String[] args) {
        new dwd_trade_order_refund().start(10006, 4, constat.TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // 1.1 读取 topic_db
        readOdsDb(tableEnv, constat.TOPIC_DWD_TRADE_ORDER_REFUND);
//        tableEnv.sqlQuery("select source from topic_table_v1").execute().print();
//| +I | {thread=NULL, ts_ms=0, conn... |
        // 1.2 读取 字典表
        readBaseDic(tableEnv);
//        tableEnv.sqlQuery("Select * from base_dic").execute().print();

        // 2. 过滤退单表数据 order_refund_info   insert
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
                        "proc_time," +
                        "ts_ms " +
                        "from topic_table_v1 " +
                        "where source['table']='order_refund_info' " +
                        "and `op`='r' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
//        | +I |                              5 |                             48 |                            239 |
//        18 |                           1502 |  1 |                        9199.00 |                           1307 |
//        退款原因具体：4892960933 |                  1743508552000 | 2025-05-05 10:56:57.697 |        1746408149185 |
//                tableEnv.sqlQuery("select * from order_refund_info ").execute().print();
//        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['province_id'] province_id," +
                        "before " +
                        "from topic_table_v1 " +
                        "where source['table']='order_info' " +
                        "and op='u'" +
                        "and before['order_status'] is not null " +
                        "and after['order_status']='1005' ");
        tableEnv.createTemporaryView("order_info", orderInfo);
//        tableEnv.sqlQuery("select *  from order_info ").execute().print();
        // 4. join: 普通的和 lookup join
//        Table result_V1 = tableEnv.sqlQuery(
//                "select " +
//                        "ri.id," +
//                        "ri.user_id," +
//                        "ri.order_id," +
//                        "ri.sku_id," +
//                        "oi.province_id," +
//                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
//                        "ri.create_time," +
//                        "ri.refund_type," +
//                        "dic1.info.dic_name," +
//                        "ri.refund_reason_type," +
//                        "dic2.info.dic_name," +
//                        "ri.refund_reason_txt," +
//                        "ri.refund_num," +
//                        "ri.refund_amount," +
//                        "ri.ts_ms " +
//                        "from order_refund_info ri " +
//                        "join order_info oi " +
//                        "on ri.order_id=oi.id " +
//                        "join base_dic for system_time as of ri.proc_time as dic1 " +
//                        "on ri.refund_type=dic1.dic_code " +
//                        "join base_dic for system_time as of ri.proc_time as dic2 " +
//                        "on ri.refund_reason_type=dic2.dic_code ");

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
                        "join base_dic for system_time as of ri.proc_time as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.proc_time as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");
        tableEnv.createTemporaryView("result_V1", result);
//        tableEnv.sqlQuery("select * from result_V1").execute().print();
//        // 5. 写出到 kafka
        tableEnv.executeSql(
                "create table " + constat.TOPIC_DWD_TRADE_ORDER_REFUND + "(" +
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
                        ")" + Sqlutil.getUpsertKafkaDDL(constat.TOPIC_DWD_TRADE_ORDER_REFUND));

        result.executeInsert(constat.TOPIC_DWD_TRADE_ORDER_REFUND);

    }

}
