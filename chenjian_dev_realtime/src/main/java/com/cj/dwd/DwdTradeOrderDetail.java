package com.cj.dwd;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cj.realtime.dwd.DwdTradeOrderDetail
 * @Author chen.jian
 * @Date 2025/4/11 11:10
 * @description: 下单事实表
 */
public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //  检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

//        从kafka的topic_db主题中读取数据
        tenv.executeSql(
                "CREATE TABLE db (\n" +
                "  before MAP<string,string>,\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  proc_time  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        过滤出订单明细数据
        Table orderDetail = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['sku_name'] sku_name," +
                        "CAST(after['create_time'] as varchar )create_time," +
                        "after['source_id'] source_id," +
                        "after['source_type'] source_type," +
                        "after['sku_num'] sku_num," +
                        "CAST(cast(after['sku_num'] as decimal(16,2)) * " +
                        "   CAST(after['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "after['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "after['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "after['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts_ms as ts " +
                        "from db " +
                        "where source['table']='order_detail' " +
                        "and `op`='c' ");
//        tenv.toChangelogStream(orderDetail).print();
        tenv.createTemporaryView("order_detail", orderDetail);

//        过滤出订单数据
        Table orderInfo = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from db " +
                        "where source['table']='order_info' " +
                        "and `op`='c' ");
//        tenv.toChangelogStream(orderInfo).print();
        tenv.createTemporaryView("order_info", orderInfo);
//        过滤出明细活动数据
        Table orderDetailActivity = tenv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['activity_id'] activity_id, " +
                        "after['activity_rule_id'] activity_rule_id " +
                        "from db " +
                        "where source['table']='order_detail_activity' " +
                        "and `op`='c' ");
//        tenv.toChangelogStream(orderDetailActivity).print();
        tenv.createTemporaryView("order_detail_activity", orderDetailActivity);

//        过滤出明细优惠券数据
        Table orderDetailCoupon = tenv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['coupon_id'] coupon_id " +
                        "from db " +
                        "where source['table']='order_detail_coupon' " +
                        "and `op`='c' ");
//        tenv.toChangelogStream(orderDetailCoupon).print();
        tenv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

//        关联上述4张表
        Table result = tenv.sqlQuery(
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
                        "date_format(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000), 'yyyy-MM-dd') AS date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");
        tenv.toChangelogStream(result).print();

//        将关联的结果写到Kafka主题
        tenv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
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
                "ts bigint," +
                "primary key(id) not enforced " +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_trade_order_detail',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        result.executeInsert("dwd_trade_order_detail");

        env.execute();

    }
}
