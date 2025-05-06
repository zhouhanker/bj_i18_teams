package com.zpy.app.dwd;

import com.zpy.constant.Constant;
import com.zpy.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD层订单退款明细处理程序
 * 功能：处理订单退款数据，关联订单信息，生成标准化的退款明细事实表
 * 主要流程：
 * 1. 从Kafka读取退款单数据和订单数据
 * 2. 过滤出退款状态的订单(状态码1006)
 * 3. 关联退款单表和订单表获取完整退款信息
 * 4. 将处理后的数据写入Kafka的DWD层主题
 */
public class DwdTradeOrderRefund {
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

        // 5. 处理退款单数据 ==============================================
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +  // 退款单ID
                        " after['user_id'] user_id," +  // 用户ID
                        " after['order_id'] order_id," +  // 订单ID
                        " after['sku_id'] sku_id," +  // 商品ID
                        " after['refund_type'] refund_type," +  // 退款类型
                        " after['refund_num'] refund_num," +  // 退款数量
                        " after['refund_amount'] refund_amount," +  // 退款金额
                        " after['refund_reason_type'] refund_reason_type," +  // 退款原因类型
                        " after['refund_reason_txt'] refund_reason_txt," +  // 退款原因说明
                        " after['create_time'] create_time," +  // 创建时间
                        " ts_ms " +  // 时间戳
                        " from topic_db " +
                        " where source['table'] = 'order_refund_info' " +  // 退款单表
                        " and `op`='r' ");  // 读取操作
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 6. 处理退款订单数据 ============================================
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +  // 订单ID
                        " after['province_id'] province_id " +  // 省份ID
                        " from topic_db " +
                        " where source['table'] = 'order_info' " +  // 订单主表
                        " and `op`='r' " +  // 读取操作
                        " and `after`['order_status'] is not null " +  // 订单状态不为空
                        " and `after`['order_status'] = '1006' ");  // 订单状态为1006(退款状态)
        tableEnv.createTemporaryView("order_info", orderInfo);

        // 7. 关联退款单和订单数据 ========================================
        Table result = tableEnv.sqlQuery(
                "select " +
                        "  ri.id," +  // 退款单ID
                        "  ri.user_id," +  // 用户ID
                        "  ri.order_id," +  // 订单ID
                        "  ri.sku_id," +  // 商品ID
                        "  oi.province_id," +  // 省份ID
                        // 格式化日期(yyyy-MM-dd)
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(ri.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        "  ri.create_time," +  // 创建时间
                        "  ri.refund_type refund_type_code," +  // 退款类型编码(重命名)
                        "  ri.refund_reason_type refund_reason_type_code," +  // 退款原因类型编码(重命名)
                        "  ri.refund_reason_txt," +  // 退款原因说明
                        "  ri.refund_num," +  // 退款数量
                        "  ri.refund_amount," +  // 退款金额
                        "  ri.ts_ms " +  // 时间戳
                        "  from order_refund_info ri " +  // 退款单数据
                        "  join order_info oi " +  // 关联订单数据
                        "  on ri.order_id=oi.id ");  // 关联条件:订单ID

        // 8. 创建Kafka结果表 ============================================
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(" +
                        "id string," +  // 退款单ID
                        "user_id string," +  // 用户ID
                        "order_id string," +  // 订单ID
                        "sku_id string," +  // 商品ID
                        "province_id string," +  // 省份ID
                        "date_id string," +  // 日期ID
                        "create_time string," +  // 创建时间
                        "refund_type_code string," +  // 退款类型编码
                        "refund_reason_type_code string," +  // 退款原因类型编码
                        "refund_reason_txt string," +  // 退款原因说明
                        "refund_num string," +  // 退款数量
                        "refund_amount string," +  // 退款金额
                        "ts_ms bigint ," +  // 时间戳
                        "PRIMARY KEY (id) NOT ENFORCED " +  // 主键(不强制)
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

        // 9. 写入结果到Kafka ============================================
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

        // 10. 执行作业 =================================================
        env.execute("DwdTradeOrderRefund");
    }
}