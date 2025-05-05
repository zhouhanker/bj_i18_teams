package com.lzy.app.dwd;

import com.lzy.constant.Constant;
import com.lzy.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.lzy.stream.realtime.v1.app.bwd.DwdTradeOrderDetail
 * @Author zheyuan.liu
 * @Date 2025/4/13 19:41
 * @description: DwdTradeOrderDetail
 */

public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        tableEnv.executeSql("select * from topic_db").print();


        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
//        tableEnv.executeSql("select * from base_dic").print();

//TODO 过滤出订单明细数据
        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "  after['id'] as id," +
                        "  after['order_id'] as order_id," +
                        "  after['sku_id'] as sku_id," +
                        "  after['sku_name'] as sku_name," +
                        "  after['create_time'] as create_time," +
                        "  after['source_id'] as source_id," +
                        "  after['source_type'] as source_type," +
                        "  after['sku_num'] as sku_num," +
                        "  cast(cast(after['sku_num'] as decimal(16,2)) * " +
                        "  cast(after['order_price'] as decimal(16,2)) as String) as split_original_amount," + // 分摊原始总金额
                        "  after['split_total_amount'] as split_total_amount," +  // 分摊总金额
                        "  after['split_activity_amount'] as split_activity_amount," + // 分摊活动金额
                        "  after['split_coupon_amount'] as split_coupon_amount," + // 分摊的优惠券金额
                        "  ts_ms " +
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail' " +
                        "  and `op`='r' ");
        tableEnv.createTemporaryView("order_detail", orderDetail);
//        orderDetail.execute().print();

        //TODO 过滤出订单数据
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "  after['id'] as id," +
                        "  after['user_id'] as user_id," +
                        "  after['province_id'] as province_id " +
                        "  from topic_db " +
                        "  where source['table'] = 'order_info' " +
                        "  and `op`='r' ");
        tableEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();

        //TODO 过滤出明细活动数据
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "  after['order_detail_id'] order_detail_id, " +
                        "  after['activity_id'] activity_id, " +
                        "  after['activity_rule_id'] activity_rule_id " +
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail_activity' " +
                        "  and `op` = 'r' ");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
//        orderDetailActivity.execute().print();

        //TODO 过滤出明细优惠券数据
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "  after['order_detail_id'] order_detail_id, " +
                        "  after['coupon_id'] coupon_id " +
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail_coupon' " +
                        "  and `op` = 'r' ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
//        orderDetailCoupon.execute().print();

        //TODO 关联上述4张表
        Table result = tableEnv.sqlQuery(
                "select " +
                        "  od.id," +
                        "  od.order_id," +
                        "  oi.user_id," +
                        "  od.sku_id," +
                        "  od.sku_name," +
                        "  oi.province_id," +
                        "  act.activity_id," +
                        "  act.activity_rule_id," +
                        "  cou.coupon_id," +
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        "  od.create_time," +
                        "  od.sku_num," +
                        "  od.split_original_amount," +
                        "  od.split_activity_amount," +
                        "  od.split_coupon_amount," +
                        "  od.split_total_amount," +
                        "  od.ts_ms " +
                        "  from order_detail od " +
                        "  join order_info oi on od.order_id = oi.id " +
                        "  left join order_detail_activity act " +
                        "  on od.id = act.order_detail_id " +
                        "  left join order_detail_coupon cou " +
                        "  on od.id = cou.order_detail_id ");
//        result.execute().print();

        //TODO 将关联的结果写到Kafka主题
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
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
                        "ts_ms bigint," +
                        "primary key(id) not enforced " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        //写入
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        env.execute("DwdOrderFactSheet");

    }
}
