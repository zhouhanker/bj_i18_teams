package com.zsf.retail_v1.realtime.dwd;


import com.zsf.retail_v1.realtime.constant.Constant;
import com.zsf.retail_v1.realtime.util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * &#064;Package  com.zsf.retail.v1.realtime.dwd.DwdTradeOrderPaySucDetail
 * &#064;Author  zhao.shuai.fei
 * &#064;Date  2025/4/10 21:33
 * &#064;description:支付成功事实表
 */
public class DwdTradeOrderPaySucDetail {
    @SneakyThrows
    public static void main(String[] args) {
        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
        // flink sql初始化
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 从下单事实表中获取下单数据
        tableEnv.executeSql(
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
                        "ts_ms bigint " +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'dwd_trade_order_detail',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'properties.group.id' = 'dwd_trade_order_cancel05',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")");
//        tableEnv.sqlQuery("select * from dwd_trade_order_detail").execute().print();

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
        //TODO 过滤出支付成功数据
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "after['user_id'] user_id," +
                "after['order_id'] order_id," +
                "after['payment_type'] payment_type," +
                "after['callback_time'] callback_time," +
                "`proc_time`," +
                "source['ts_ms'] ts_ms " +
                "from topic_db " +
                "where `source`['table']='payment_info' " +
                "and `op`='u' " +
                "and `before`['payment_status'] is not null " +
                "and `after`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);
//        paymentInfo.execute().print();
        //TODO 从HBase中读取字典数据 创建动态表
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
        // 和字典进行关联---lookup join 和下单数据进行关联---IntervalJoin
        // 略加修改                 "and od.et >= pi.et - interval '30' minute " +
        //                        "and od.et <= pi.et + interval '5' second " +
        Table result = tableEnv.sqlQuery(
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
                        "join base_dic for system_time as of pi.proc_time as dic " +
                        "on pi.payment_type=dic.dic_code ");

         tableEnv.createTemporaryView("result", result);
//         result.execute().print();
//        //TODO 将关联的结果写到kafka主题中
        tableEnv.executeSql("create table "+ Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+"(" +
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
                "ts_ms string ," +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

//        env.execute("dwd06");
    }
}
