package com.bg.realtime_dwd.base_db.APP;

import com.bg.common.base.BaseSQLApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.bg.realtime_dwd.base_db.app.DwdTradeOrderDetail
 * @Author Chen.Run.ze
 * @Date 2025/4/11 13:32
 * @description: 下单事实表
 */
public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 设置状态的保留时间[传输的延迟 + 业务上的滞后关系]
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        //TODO 从kafka的topic_db主题中读取数据 创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
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
                        "cast(after['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "after['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "after['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "after['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts_ms " +
                        "from topic_db " +
                        "where `source`['table']='order_detail' " +
                        "and `op`='c' ");
        tableEnv.createTemporaryView("order_detail", orderDetail);

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
                        "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(od.create_time AS BIGINT), 3), 'yyyy-MM-dd') date_id," +  // 年月日
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

        //TODO 写入
        //+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+
        //| op |                             id |                       order_id |                        user_id |                         sku_id |                       sku_name |                    province_id |                    activity_id |               activity_rule_id |                      coupon_id |                        date_id |                    create_time |                        sku_num |          split_original_amount |          split_activity_amount |            split_coupon_amount |             split_total_amount |                ts_ms |
        //+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+
        //| +I |                           3447 |                           2432 |                            459 |                             24 |  金沙河面条 原味银丝挂面 龙... |                              5 |                         <NULL> |                         <NULL> |                         <NULL> |                     2025-04-28 |                  1745852856000 |                              1 |                        11.0000 |                            0.0 |                            0.0 |                           11.0 |        1745824056241 |
        //| +I |                           3448 |                           2433 |                            638 |                              6 |  Redmi 10X 4G Helio G85游戏... |                             27 |                         <NULL> |                         <NULL> |                         <NULL> |                     2025-04-28 |                  1745852856000 |                              1 |                      1299.0000 |                            0.0 |                            0.0 |                         1299.0 |        1745824056313 |
        //| +I |                           3483 |                           2459 |                            480 |                             27 | 索芙特i-Softto 口红不掉色唇... |                             22 |                         <NULL> |                         <NULL> |                              1 |                     2025-04-28 |                  1745852857000 |                              1 |                       129.0000 |                            0.0 |                           30.0 |                           99.0 |        1745824057786 |
        //| +I |                           3498 |                           2469 |                            403 |                             35 | 华为智慧屏V65i 65英寸 HEGE-... |                             11 |                         <NULL> |                         <NULL> |                         <NULL> |                     2025-04-28 |                  1745852858000 |                              1 |                      5499.0000 |                            0.0 |                            0.0 |                         5499.0 |        1745824058142 |
//        result.execute().print();
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
