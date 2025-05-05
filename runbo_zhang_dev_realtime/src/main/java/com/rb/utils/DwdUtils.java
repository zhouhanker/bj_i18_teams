package com.rb.utils;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.rb.utils.DwdUtils
 * @Author runbo.zhang
 * @Date 2025/4/11 18:47
 * @description:
 */
public class DwdUtils {
    public static void dwdKafkaDbInit(StreamTableEnvironment tEnv, String topic){
        tEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `source` MAP<string, string>,\n" +
                "  `op` string,\n" +
                "  `ts_ms` bigint,\n" +
                "  `after` MAP<string, string>,\n" +

                "  `before` MAP<string, string>,\n" +
                "  pt as proctime(),\n" +//处理时间
                "  et as to_timestamp_ltz(ts_ms, 3), " +
                "  watermark for et as et - interval '3' second " +
                ") " + SQLUtil.getKafkaDDL(topic));
//        tEnv.executeSql("select * from topic_db").print();
    }
    public static void hbaseBaseDicInit(StreamTableEnvironment tEnv,String namespace,String table){
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL(namespace,table));

//        tEnv.executeSql("select * from base_dic").print();
    }
    public static void orderInfoRefundByStatus(StreamTableEnvironment tEnv,String status){
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['province_id'] province_id," +
                        "`after`['user_id'] user_id," +
                        "`before` " +
                        "from topic_db " +
                        "where `source`['table']='order_info' " +
                        "and `op`='u'" +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='"+status+"' ");
        tEnv.createTemporaryView("order_info", orderInfo);
    }
    public static void orderRefundInfoUtil(StreamTableEnvironment tEnv,String status,String op){
        Table orderRefundInfo=null;
        if("c".equals(op)){
             orderRefundInfo = tEnv.sqlQuery(
                    "select " +
                            "`after`['id'] id," +
                            "`after`['user_id'] user_id," +
                            "`after`['order_id'] order_id," +
                            "`after`['sku_id'] sku_id," +
                            "`after`['refund_type'] refund_type," +
                            "`after`['refund_num'] refund_num," +
                            "`after`['refund_amount'] refund_amount," +
                            "`after`['refund_reason_type'] refund_reason_type," +
                            "`after`['refund_reason_txt'] refund_reason_txt," +
                            "`after`['create_time'] create_time," +
                            "pt," +
                            "`source`['ts_ms'] ts_ms " +
                            "from topic_db " +
                            "where `source`['table']='order_refund_info' " +
                            "and `op`='c' ");

        }else if ("u".equals(op)){
             orderRefundInfo = tEnv.sqlQuery(
                    "select " +
                            "`after`['order_id'] order_id," +
                            "`after`['sku_id'] sku_id," +
                            "`after`['refund_num'] refund_num " +
                            "from topic_db " +
                            "where `source`['table']='order_refund_info' " +
                            "and `op`='u' " +
                            "and `before`['refund_status'] is not null " +
                            "and `after`['refund_status']='"+status+"'");

        }
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);
    }
}

