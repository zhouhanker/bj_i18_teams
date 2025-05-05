package com.zsf.retail_v1.realtime.dwd;


import com.zsf.retail_v1.realtime.constant.Constant;
import com.zsf.retail_v1.realtime.util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * &#064;Package  com.zsf.retail.v1.realtime.dwd.DwdTradeOrderRefund
 * &#064;Author  zhao.shuai.fei
 * &#064;Date  2025/4/11 9:04
 * &#064;description:  退款事务表
 */
public class DwdTradeOrderRefund {
    @SneakyThrows
    public static void main(String[] args) {
        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
        // flink sql初始化
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 从kafka的topic_db主题中读取数据 创建动态表
        tableEnv.executeSql("create table topic_db(\n" +
                "    `before` map<string,string>,\n" +
                "    `after` map<string,string>,\n" +
                "    `source` map<string,string>,\n" +
                "    `op` string,\n" +
                "    `ts_ms` BIGINT,\n" +
                "    proc_time as proctime()\n" +
                ")WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup03',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_db limit 5").print();

        // 1.2 读取 字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + Constant.HBASE_NAMESPACE + ":dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181,cdh02:2181,cdh03:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")");
//        tableEnv.executeSql("select * from base_dic").print();

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
                        "source['ts_ms'] ts_ms " +
                        "from topic_db " +
                        "where `source`['table']='order_refund_info' " +
                        "and `op`='c' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
//        orderRefundInfo.execute().print();

        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['province_id'] province_id," +
                        "`before` " +
                        "from topic_db " +
                        "where `source`['table']='order_info' " +
                        "and `op`='u'" +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1005' ");
        tableEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();
        // 4. join: 普通的和 lookup join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
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
//        result.execute().print();
        // 5. 写出到 kafka
        tableEnv.executeSql(
                "create table "+ Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(" +
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
                        "ts_ms string ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

//        env.execute("dwd07");
    }
}
