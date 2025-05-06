package com.cj.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cj.realtime.dwd.DwdTradeRefundPaySucDetail
 * @Author chen.jian
 * @Date 2025/4/11 19:48
 * @description: 退款成功事实表
 */
public class DwdTradeRefundPaySucDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

//        读取 topic_db
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  before MAP<string,string>,\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  proc_time  AS proctime(),\n "+
                "  et AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" +
                "  WATERMARK FOR et AS et - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        读取 字典表
        tenv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code String,\n" +
                " info ROW<dic_name String>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall_config:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181'\n" +
                ");");

//        过滤退款成功表数据
        Table refundPayment = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['payment_type'] payment_type," +
                        "after['callback_time'] callback_time," +
                        "after['total_amount'] total_amount," +
                        "proc_time as pt, " +
                        "ts_ms as ts " +
                        "from db " +
                        "where source['table']='refund_payment' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='1602'");
        tenv.createTemporaryView("refund_payment", refundPayment);

//        过滤退单表中的退单成功的数据
        Table orderRefundInfo = tenv.sqlQuery(
                "select " +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['refund_num'] refund_num " +
                        "from db " +
                        "where source['table']='order_refund_info' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='0705'");

        tenv.createTemporaryView("order_refund_info", orderRefundInfo);

//        过滤订单表中的退款成功的数据
        Table orderInfo = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from db " +
                        "where source['table']='order_info' " +
                        "and `op`='u' " +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1006'");
        tenv.createTemporaryView("order_info", orderInfo);

//        4 张表的 join
        Table result = tenv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(FROM_UNIXTIME(CAST(rp.callback_time AS BIGINT) / 1000), 'yyyy-MM-dd') AS date_id," +
                        "date_format(FROM_UNIXTIME(CAST(rp.callback_time AS BIGINT) / 1000), 'yyyy-MM-dd hh:mm:ss') AS callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " +
                        "on rp.payment_type=dic.dic_code ");
        tenv.toChangelogStream(result).print();

//        写出到 kafka
        tenv.executeSql(
                "create table dwd_trade_refund_payment_success(" +
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
                        "ts bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ") " +
                        " WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'dwd_trade_refund_payment_success',\n" +
                        "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")");
        result.executeInsert("dwd_trade_refund_payment_success");


        env.execute();
    }
}
