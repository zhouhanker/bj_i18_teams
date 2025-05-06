package com.zsf.retail_v1.realtime.dwd;


import com.zsf.retail_v1.realtime.constant.Constant;
import com.zsf.retail_v1.realtime.util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * &#064;Package  com.zsf.retail.v1.realtime.dwd.DwdTradeOrderDetail
 * &#064;Author  zhao.shuai.fei
 * &#064;Date  2025/4/10 19:34
 * &#064;description:下单事实表
 */
public class DwdTradeOrderDetail {
    @SneakyThrows
    public static void main(String[] args) {
        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(1);
        // flink sql初始化
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 设置状态的保留时间[传输的延迟 + 业务上的滞后关系]
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
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
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_db limit 5").print();

        //TODO 过滤出订单明细数据
        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['sku_name'] sku_name," +
                        "after['create_time'] create_time," +
                        "after['source_id'] source_id," +
                        "after['source_type'] source_type," +
                        "after['sku_num'] sku_num," +
                        "cast(cast(after['sku_num'] as decimal(16,2)) * " +
                        "   cast(after['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "after['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "after['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "after['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "source['ts_ms'] ts_ms " +
                        "from topic_db " +
                        "where `source`['table']='order_detail' " +
                        "and `op`='c' ");
        tableEnv.createTemporaryView("order_detail", orderDetail);

//        tableEnv.executeSql("select * from topic_db").print();

        //TODO 过滤出订单数据
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from topic_db " +
                        "where `source`['table']='order_info' " +
                        "and `op`='c' ");
        tableEnv.createTemporaryView("order_info", orderInfo);

        //TODO 过滤出明细活动数据
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['activity_id'] activity_id, " +
                        "after['activity_rule_id'] activity_rule_id " +
                        "from topic_db " +
                        "where `source`['table']='order_detail_activity' " +
                        "and `op`='c' ");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        //TODO 过滤出明细优惠券数据
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['coupon_id'] coupon_id " +
                        "from topic_db " +
                        "where `source`['table']='order_detail_coupon' " +
                        "and `op`='c' ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        //TODO 关联上述4张表
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
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
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

        //TODO 将关联的结果写到Kafka主题
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql(
                "create table "+ Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
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
                        "ts_ms string," +
                        "primary key(id) not enforced " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        //写入
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
//        env.execute("dwd04");
    }
}
