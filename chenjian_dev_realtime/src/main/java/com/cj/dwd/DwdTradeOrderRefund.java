package com.cj.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cj.realtime.dwd.DwdTradeOrderRefund
 * @Author chen.jian
 * @Date 2025/4/11 18:34
 * @description: 退单事实表
 */
public class DwdTradeOrderRefund {
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


//        过滤退单表数据
        Table orderRefundInfo = tenv.sqlQuery(
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
                        "proc_time as pt," +
                        "ts_ms as ts " +
                        "from db " +
                        "where source['table']='order_refund_info'");
        tenv.createTemporaryView("order_refund_info", orderRefundInfo);

//        过滤订单表中的退单数据
        Table orderInfo = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['province_id'] province_id," +
                        "`before` " +
                        "from db " +
                        "where source['table']='order_info' " +
                        "and `op`='u'" +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1005' ");
        tenv.createTemporaryView("order_info", orderInfo);

//        普通的和 lookup join
        Table result = tenv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(FROM_UNIXTIME(CAST(ri.create_time AS BIGINT) / 1000), 'yyyy-MM-dd') AS date_id," +
                        "date_format(FROM_UNIXTIME(CAST(ri.create_time AS BIGINT) / 1000), 'yyyy-MM-dd hh:mm:ss') AS create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");
        tenv.toChangelogStream(result).print();

//        写出到 kafka
        tenv.executeSql(
                "create table dwd_trade_order_refund(" +
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
                        "ts bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ") " +
                        " WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'dwd_trade_order_refund',\n" +
                        "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")");
//        result.executeInsert("dwd_trade_order_refund");


        env.execute();
    }
}

