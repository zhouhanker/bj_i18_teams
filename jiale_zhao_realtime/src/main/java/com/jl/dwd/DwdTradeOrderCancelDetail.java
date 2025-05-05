package com.jl.dwd;



import com.jl.constant.Constant;
import com.jl.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.jl.DwdTradeOrderCancelDetail
 * @Author jia.le
 * @Date 2025/4/13 18:53
 * @description: DwdTradeOrderCancelDetail
 */

public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) throws Exception {

        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
        // flink sql初始化
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 设置状态的保留时间
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        //TODO 从kafka的topic_db主题中读取数据
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
                "  'properties.group.id' = 'testGroup01',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_db limit 5").print();

        //TODO 过滤出取消订单行为
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " `source`['ts_ms'] ts_ms " +
                "from topic_db " +
                "where `source`['table']='order_info' " +
                "and `op`='u' " +
                "and `before`['order_status']='1001' " +
                "and `after`['order_status']='1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);
//        orderCancel.execute().print();

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
                        "  'properties.group.id' = 'dwd_trade_order_cancel03',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")");
//        tableEnv.sqlQuery("select * from dwd_trade_order_detail").execute().print();

        //TODO 将下单事实表和取消订单表进行关联
        Table result = tableEnv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts_ms " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");
//        result.execute().print();

        //TODO 将关联的结果写到kafka主题中
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
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
                        "ts_ms string ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
