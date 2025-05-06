package com.zpy.app.dwd;

import com.zpy.constant.Constant;
import com.zpy.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD层订单取消明细处理程序
 * 功能：处理订单取消数据，关联订单明细数据，生成完整的订单取消明细记录
 *主要任务:
 *关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表的insert操作，
 * 形成下单明细表，写入 Kafka 对应主题。
 *
 * 主要流程：
 * 1. 从Kafka读取订单原始数据
 * 2. 过滤出取消状态的订单(状态码1001或1003)
 * 3. 关联DWD层订单明细表获取完整订单信息
 * 4. 将处理后的数据写入Kafka的DWD层主题
 */

public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) throws Exception {
        // 1. 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为4
        env.setParallelism(4);

        // 创建Table API环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 启用检查点，每5秒一次，使用EXACTLY_ONCE语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置状态保留时间为30分钟5秒(用于join操作的状态保留)
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 2. 创建Kafka源表(订单原始数据)
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +  // 变更后的数据
                "  source MAP<string, string>, \n" +  // 源表信息
                "  `op` string, \n" +  // 操作类型
                "  ts_ms bigint " +  // 时间戳
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 3. 创建HBase维表(字典表)
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +  // 字典编码
                " info ROW<dic_name string>,\n" +  // 字典名称
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +  // 主键
                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));

        // 4. 提取取消订单数据
        // 从topic_db中筛选order_info表的取消状态订单(状态码1001或1003)
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] as id, " +  // 订单ID
                " `after`['operate_time'] as operate_time, " +  // 操作时间
                " `ts_ms` " +  // Kafka消息时间戳
                " from topic_db " +
                " where source['table'] = 'order_info' " +  // 订单表
                " and `op` = 'r' " +  // 读取操作
                " and (`after`['order_status'] = '1001' " +  // 订单状态1001(已取消)
                " or `after`['order_status'] = '1003') ");  // 或1003(已退款)

        // 创建取消订单临时视图
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        // 5. 创建订单明细事实表
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "  id string," +  // 订单明细ID
                        "  order_id string," +  // 订单ID
                        "  user_id string," +  // 用户ID
                        "  sku_id string," +  // 商品ID
                        "  sku_name string," +  // 商品名称
                        "  province_id string," +  // 省份ID
                        "  activity_id string," +  // 活动ID
                        "  activity_rule_id string," +  // 活动规则ID
                        "  coupon_id string," +  // 优惠券ID
                        "  date_id string," +  // 日期ID
                        "  create_time string," +  // 创建时间
                        "  sku_num string," +  // 商品数量
                        "  split_original_amount string," +  // 原始金额
                        "  split_activity_amount string," +  // 活动优惠金额
                        "  split_coupon_amount string," +  // 优惠券优惠金额
                        "  split_total_amount string," +  // 总金额
                        "  ts_ms bigint " +  // 时间戳
                        "  )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 6. 关联订单明细和取消订单
        Table result = tableEnv.sqlQuery(
                "select  " +
                        "  od.id," +  // 订单明细ID
                        "  od.order_id," +  // 订单ID
                        "  od.user_id," +  // 用户ID
                        "  od.sku_id," +  // 商品ID
                        "  od.sku_name," +  // 商品名称
                        "  od.province_id," +  // 省份ID
                        "  od.activity_id," +  // 活动ID
                        "  od.activity_rule_id," +  // 活动规则ID
                        "  od.coupon_id," +  // 优惠券ID
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +  // 格式化日期
                        "  oc.operate_time as cancel_time," +  // 取消时间(重命名)
                        "  od.sku_num," +  // 商品数量
                        "  od.split_original_amount," +  // 原始金额
                        "  od.split_activity_amount," +  // 活动优惠金额
                        "  od.split_coupon_amount," +  // 优惠券优惠金额
                        "  od.split_total_amount," +  // 总金额
                        "  oc.ts_ms " +  // 时间戳
                        "  from dwd_trade_order_detail od " +  // 订单明细表
                        "  join order_cancel oc " +  // 关联取消订单表
                        "  on od.order_id = oc.id ");  // 关联条件:订单ID相等

        // 7. 创建Kafka结果表(订单取消明细)
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
                        "  id string," +  // 订单明细ID
                        "  order_id string," +  // 订单ID
                        "  user_id string," +  // 用户ID
                        "  sku_id string," +  // 商品ID
                        "  sku_name string," +  // 商品名称
                        "  province_id string," +  // 省份ID
                        "  activity_id string," +  // 活动ID
                        "  activity_rule_id string," +  // 活动规则ID
                        "  coupon_id string," +  // 优惠券ID
                        "  date_id string," +  // 日期ID
                        "  cancel_time string," +  // 取消时间
                        "  sku_num string," +  // 商品数量
                        "  split_original_amount string," +  // 原始金额
                        "  split_activity_amount string," +  // 活动优惠金额
                        "  split_coupon_amount string," +  // 优惠券优惠金额
                        "  split_total_amount string," +  // 总金额
                        "  ts_ms bigint ," +  // 时间戳
                        "  PRIMARY KEY (id) NOT ENFORCED " +  // 主键(不强制)
                        "  )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 8. 写入结果到Kafka
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

        // 9. 执行作业
        env.execute("DwdTradeOrderCancelDetail");
    }
}