package com.zpy.stream.realtime.v1.app.dwd;

import com.zpy.stream.realtime.v1.constant.Constant;
import com.zpy.stream.realtime.v1.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD层支付成功订单明细处理程序
 * 功能：处理支付成功订单数据，关联订单明细数据，生成支付成功订单明细事实表
 * 主要流程：
 * 1. 从Kafka读取支付成功数据
 * 2. 关联订单明细表获取完整订单信息
 * 3. 将处理后的数据写入Kafka的DWD层主题
 * 特点：
 * - 精确一次处理语义
 * - 状态保留机制确保关联准确性
 * - 生成标准化的支付成功订单明细数据
 */
public class DwdTradeOrderPaySucDetail {
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

        // 5. 创建订单明细事实表视图 ========================================
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        " id string," +  // 订单明细ID
                        " order_id string," +  // 订单ID
                        " user_id string," +  // 用户ID
                        " sku_id string," +  // 商品ID
                        " sku_name string," +  // 商品名称
                        " province_id string," +  // 省份ID
                        " activity_id string," +  // 活动ID
                        " activity_rule_id string," +  // 活动规则ID
                        " coupon_id string," +  // 优惠券ID
                        " date_id string," +  // 日期ID
                        " create_time string," +  // 创建时间
                        " sku_num string," +  // 商品数量
                        " split_original_amount string," +  // 原始金额
                        " split_activity_amount string," +  // 活动优惠金额
                        " split_coupon_amount string," +  // 优惠券优惠金额
                        " split_total_amount string," +  // 总金额
                        " ts_ms bigint " +  // 时间戳
                        " )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        // 6. 过滤支付成功数据 =============================================
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "after['user_id'] user_id," +  // 用户ID
                "after['order_id'] order_id," +  // 订单ID
                "after['payment_type'] payment_type," +  // 支付类型
                "after['callback_time'] callback_time," +  // 回调时间(支付成功时间)
                "ts_ms " +  // 时间戳
                "from topic_db " +
                "where source['table'] ='payment_info' " +  // 支付信息表
                "and `op`='r' " +  // 读取操作
                "and `after`['payment_status'] is not null " +  // 支付状态不为空
                "and `after`['payment_status'] = '1602' ");  // 支付状态为1602(支付成功)
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // 7. 关联订单明细数据 =============================================
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +  // 订单明细ID
                        "od.order_id," +  // 订单ID
                        "od.user_id," +  // 用户ID
                        "od.sku_id," +  // 商品ID
                        "od.sku_name," +  // 商品名称
                        "od.province_id," +  // 省份ID
                        "od.activity_id," +  // 活动ID
                        "od.activity_rule_id," +  // 活动规则ID
                        "od.coupon_id," +  // 优惠券ID
                        "pi.payment_type payment_type_code ," +  // 支付类型编码
                        "pi.callback_time," +  // 支付成功时间
                        "od.sku_num," +  // 商品数量
                        "od.split_original_amount," +  // 原始金额
                        "od.split_activity_amount," +  // 活动优惠金额
                        "od.split_coupon_amount," +  // 优惠券优惠金额
                        "od.split_total_amount split_payment_amount," +  // 支付金额(重命名)
                        "pi.ts_ms " +  // 时间戳
                        "from payment_info pi " +  // 支付成功数据
                        "join dwd_trade_order_detail od " +  // 关联订单明细
                        "on pi.order_id = od.order_id ");  // 关联条件:订单ID

        // 8. 创建Kafka结果表 =============================================
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+"(" +
                "order_detail_id string," +  // 订单明细ID
                "order_id string," +  // 订单ID
                "user_id string," +  // 用户ID
                "sku_id string," +  // 商品ID
                "sku_name string," +  // 商品名称
                "province_id string," +  // 省份ID
                "activity_id string," +  // 活动ID
                "activity_rule_id string," +  // 活动规则ID
                "coupon_id string," +  // 优惠券ID
                "payment_type_code string," +  // 支付类型编码
                "callback_time string," +  // 支付成功时间
                "sku_num string," +  // 商品数量
                "split_original_amount string," +  // 原始金额
                "split_activity_amount string," +  // 活动优惠金额
                "split_coupon_amount string," +  // 优惠券优惠金额
                "split_payment_amount string," +  // 支付金额
                "ts_ms bigint ," +  // 时间戳
                "PRIMARY KEY (order_detail_id) NOT ENFORCED " +  // 主键(不强制)
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        // 9. 写入结果到Kafka =============================================
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        // 10. 执行作业 ==================================================
        env.execute("DwdTradeOrderPaySucDetail");
    }
}