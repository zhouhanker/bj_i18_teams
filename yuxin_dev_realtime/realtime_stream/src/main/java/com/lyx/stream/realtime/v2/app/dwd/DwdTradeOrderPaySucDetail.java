package com.lyx.stream.realtime.v2.app.dwd;


import com.lyx.stream.realtime.v2.app.constant.Constant;
import com.lyx.stream.realtime.v2.app.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.lyx.stream.realtime.v2.app.bwd.DwdTradeOrderPaySucDetail
 * @Author yuxin_li
 * @Date 2025/4/14 11:04
 * @description: DwdTradeOrderPaySucDetail
 * 从 Kafka 中读取下单事实表数据和支付信息数据
 * 过滤出支付成功的数据，将其与下单详情数据进行关联
 * 最后将关联结果写入到另一个 Kafka 主题中
 */

public class DwdTradeOrderPaySucDetail {
    public static void main(String[] args) throws Exception {
                // 创建流式执行环境，它是 Flink 流式处理程序的入口点
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为 4，意味着 Flink 会使用 4 个并行的任务来处理数据
                env.setParallelism(4);
        // 创建流表执行环境，用于执行 Flink SQL 语句
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 启用检查点机制，检查点间隔为 5000 毫秒，采用精确一次（EXACTLY_ONCE）语义保证数据处理的准确性
                env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置空闲状态的保留时间为 30 分钟零 5 秒，超过该时间的空闲状态会被清理
                tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 创建一个名为 topic_db 的 Kafka 源表，用于从 Kafka 主题中读取数据
        // 表结构包含 after（键值对映射）、source（键值对映射）、op（操作类型）和 ts_ms（时间戳）字段
        // SQLUtil.getKafkaDDL 方法用于生成 Kafka 相关的 DDL 配置
                tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                        "  after MAP<string, string>, \n" +
                        "  source MAP<string, string>, \n" +
                        "  `op` string, \n" +
                        "  ts_ms bigint " +
                        ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 可以取消注释来打印 topic_db 表中的所有数据，用于调试
        // tableEnv.executeSql("select * from topic_db").print();

        // 创建一个名为 base_dic 的 HBase 维度表，用于存储字典数据
        // 表结构包含 dic_code（字典代码）和 info（包含 dic_name 字段的行）字段
        // SQLUtil.getHBaseDDL 方法用于生成 HBase 相关的 DDL 配置
                tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                        " dic_code string,\n" +
                        " info ROW<dic_name string>,\n" +
                        " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                        ") " + SQLUtil.getHBaseDDL("dim_base_dic")
                );
        // 可以取消注释来打印 base_dic 表中的所有数据，用于调试
        // tableEnv.executeSql("select * from base_dic").print();

        // 从 Kafka 主题中读取下单事实表数据，创建名为 dwd_trade_order_detail 的动态表
        // 表结构包含订单详情的各种信息，如订单 ID、用户 ID、商品 ID 等
        // SQLUtil.getKafkaDDL 方法用于生成 Kafka 相关的 DDL 配置
                tableEnv.executeSql(
                        "create table dwd_trade_order_detail(" +
                                " id string," +
                                " order_id string," +
                                " user_id string," +
                                " sku_id string," +
                                " sku_name string," +
                                " province_id string," +
                                " activity_id string," +
                                " activity_rule_id string," +
                                " coupon_id string," +
                                " date_id string," +
                                " create_time string," +
                                " sku_num string," +
                                " split_original_amount string," +
                                " split_activity_amount string," +
                                " split_coupon_amount string," +
                                " split_total_amount string," +
                                " ts_ms bigint " +
                                " )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        // 从 topic_db 表中过滤出支付成功的数据，创建一个名为 paymentInfo 的表
        // 筛选条件为 source 表名为 payment_info，操作类型为 r，且支付状态为 1602
        // 提取用户 ID、订单 ID、支付类型、回调时间和时间戳等字段
                Table paymentInfo = tableEnv.sqlQuery("select " +
                        "after['user_id'] user_id," +
                        "after['order_id'] order_id," +
                        "after['payment_type'] payment_type," +
                        "after['callback_time'] callback_time," +
                        "ts_ms " +
                        "from topic_db " +
                        "where source['table'] ='payment_info' " +
                        "and `op`='r' " +
                        "and `after`['payment_status'] is not null " +
                        "and `after`['payment_status'] = '1602' ");
        // 将 paymentInfo 表注册为临时视图，方便后续 SQL 查询使用
                tableEnv.createTemporaryView("payment_info", paymentInfo);
        // 可以取消注释来打印 paymentInfo 表中的数据，用于调试
        // paymentInfo.execute().print();

        // 将支付成功的数据（payment_info 视图）和下单详情数据（dwd_trade_order_detail 表）进行关联
        // 关联条件为订单 ID 相等，提取所需的字段，如订单详情 ID、订单 ID、用户 ID 等
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
                                "pi.callback_time," +
                                "od.sku_num," +
                                "od.split_original_amount," +
                                "od.split_activity_amount," +
                                "od.split_coupon_amount," +
                                "od.split_total_amount split_payment_amount," +
                                "pi.ts_ms " +
                                "from payment_info pi " +
                                "join dwd_trade_order_detail od " +
                                "on pi.order_id = od.order_id ");
        // 可以取消注释来打印关联结果，用于调试
        // result.execute().print();

        // 创建一个名为 Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS 的 Kafka 目标表
        // 表结构包含关联结果的各个字段，如订单详情 ID、订单 ID、用户 ID 等
        // SQLUtil.getUpsertKafkaDDL 方法用于生成支持 Upsert 操作的 Kafka DDL 配置
                tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+"(" +
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
                        "callback_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_payment_amount string," +
                        "ts_ms bigint ," +
                        "PRIMARY KEY (order_detail_id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        // 将关联结果插入到 Kafka 目标表中
                result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);


                env.execute("DwdTradeOrderPaySucDetail");
    }
}
