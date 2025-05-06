package com.zpy.app.dwd;

import com.zpy.constant.Constant;
import com.zpy.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD层订单明细处理程序
 * 功能：构建订单明细事实表，关联订单主表、活动表和优惠券表，生成完整的订单明细数据
 * 主要流程：
 * 1. 从Kafka读取订单相关原始数据
 * 2. 分别过滤出订单明细、订单主表、活动关联和优惠券关联数据
 * 3. 关联四张表获取完整订单明细信息
 * 4. 将处理后的数据写入Kafka的DWD层主题
 */
public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {
        // 1. 初始化流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // 设置并行度为4
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 检查点配置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE); // 5秒一次检查点，精确一次语义
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5)); // 状态保留30分5秒

        // 3. 创建Kafka源表(数据库变更数据)
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +  // 变更后的数据
                "  source MAP<string, string>, \n" +  // 源表信息
                "  `op` string, \n" +  // 操作类型
                "  ts_ms bigint " +  // 时间戳
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 4. 创建HBase维表(字典表)
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +  // 字典编码
                " info ROW<dic_name string>,\n" +  // 字典名称
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +  // 主键
                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));

        // 5. 处理订单明细数据 ==============================================
        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "  after['id'] as id," +  // 订单明细ID
                        "  after['order_id'] as order_id," +  // 订单ID
                        "  after['sku_id'] as sku_id," +  // 商品ID
                        "  after['sku_name'] as sku_name," +  // 商品名称
                        "  after['create_time'] as create_time," +  // 创建时间
                        "  after['source_id'] as source_id," +  // 来源ID
                        "  after['source_type'] as source_type," +  // 来源类型
                        "  after['sku_num'] as sku_num," +  // 商品数量
                        // 计算原始金额 = 数量 * 单价
                        "  cast(cast(after['sku_num'] as decimal(16,2)) * " +
                        "  cast(after['order_price'] as decimal(16,2)) as String) as split_original_amount," +
                        "  after['split_total_amount'] as split_total_amount," +  // 分摊总金额
                        "  after['split_activity_amount'] as split_activity_amount," +  // 分摊活动优惠
                        "  after['split_coupon_amount'] as split_coupon_amount," +  // 分摊优惠券优惠
                        "  ts_ms " +  // 时间戳
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail' " +  // 订单明细表
                        "  and `op`='r' ");  // 读取操作
        tableEnv.createTemporaryView("order_detail", orderDetail);

        // 6. 处理订单主表数据 ==============================================
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "  after['id'] as id," +  // 订单ID
                        "  after['user_id'] as user_id," +  // 用户ID
                        "  after['province_id'] as province_id " +  // 省份ID
                        "  from topic_db " +
                        "  where source['table'] = 'order_info' " +  // 订单主表
                        "  and `op`='r' ");  // 读取操作
        tableEnv.createTemporaryView("order_info", orderInfo);

        // 7. 处理订单活动关联数据 ==========================================
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "  after['order_detail_id'] order_detail_id, " +  // 订单明细ID
                        "  after['activity_id'] activity_id, " +  // 活动ID
                        "  after['activity_rule_id'] activity_rule_id " +  // 活动规则ID
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail_activity' " +  // 订单活动关联表
                        "  and `op` = 'r' ");  // 读取操作
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 8. 处理订单优惠券关联数据 ========================================
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "  after['order_detail_id'] order_detail_id, " +  // 订单明细ID
                        "  after['coupon_id'] coupon_id " +  // 优惠券ID
                        "  from topic_db " +
                        "  where source['table'] = 'order_detail_coupon' " +  // 订单优惠券关联表
                        "  and `op` = 'r' ");  // 读取操作
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 9. 关联四张表 ==================================================
        Table result = tableEnv.sqlQuery(
                "select " +
                        "  od.id," +  // 订单明细ID
                        "  od.order_id," +  // 订单ID
                        "  oi.user_id," +  // 用户ID
                        "  od.sku_id," +  // 商品ID
                        "  od.sku_name," +  // 商品名称
                        "  oi.province_id," +  // 省份ID
                        "  act.activity_id," +  // 活动ID
                        "  act.activity_rule_id," +  // 活动规则ID
                        "  cou.coupon_id," +  // 优惠券ID
                        // 格式化日期(yyyy-MM-dd)
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        "  od.create_time," +  // 创建时间
                        "  od.sku_num," +  // 商品数量
                        "  od.split_original_amount," +  // 原始金额
                        "  od.split_activity_amount," +  // 活动优惠金额
                        "  od.split_coupon_amount," +  // 优惠券优惠金额
                        "  od.split_total_amount," +  // 总金额
                        "  od.ts_ms " +  // 时间戳
                        "  from order_detail od " +  // 主表:订单明细
                        "  join order_info oi on od.order_id = oi.id " +  // 关联订单主表
                        "  left join order_detail_activity act " +  // 左关联活动表
                        "  on od.id = act.order_detail_id " +
                        "  left join order_detail_coupon cou " +  // 左关联优惠券表
                        "  on od.id = cou.order_detail_id ");

        // 10. 创建Kafka结果表 ============================================
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
                        "id string," +  // 订单明细ID
                        "order_id string," +  // 订单ID
                        "user_id string," +  // 用户ID
                        "sku_id string," +  // 商品ID
                        "sku_name string," +  // 商品名称
                        "province_id string," +  // 省份ID
                        "activity_id string," +  // 活动ID
                        "activity_rule_id string," +  // 活动规则ID
                        "coupon_id string," +  // 优惠券ID
                        "date_id string," +  // 日期ID
                        "create_time string," +  // 创建时间
                        "sku_num string," +  // 商品数量
                        "split_original_amount string," +  // 原始金额
                        "split_activity_amount string," +  // 活动优惠金额
                        "split_coupon_amount string," +  // 优惠券优惠金额
                        "split_total_amount string," +  // 总金额
                        "ts_ms bigint," +  // 时间戳
                        "primary key(id) not enforced " +  // 主键(不强制)
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        // 11. 写入结果到Kafka ============================================
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        // 12. 执行作业 ==================================================
        env.execute("DwdOrderFactSheet");
    }
}