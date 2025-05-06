package com.zpy.stream.realtime.v1.app.dwd;

import com.zpy.stream.realtime.v1.constant.Constant;
import com.zpy.stream.realtime.v1.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD层交易域退款成功明细表
 * 主要任务：
 * 1. 从退款表提取退款成功数据
 * 2. 从订单表提取退款成功订单数据
 * 3. 从退单表提取退款成功明细数据
 * 4. 关联维度表并写入Kafka
 *
 * @author pengyu_zhu
 */
public class DwdTradeRefundPaySucDetail {
    public static void main(String[] args) throws Exception {
        // 1. 初始化流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // 设置并行度
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 检查点配置（精确一次语义，间隔5秒）
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 状态保留时间（30分钟）
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 2. 创建Kafka源表（监听数据库变更日志）
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +  // 变更后的数据
                "  source MAP<string, string>, \n" + // 源表信息
                "  `op` string, \n" +               // 操作类型（r=读取，c=创建，u=更新）
                "  ts_ms bigint " +                 // 变更时间戳
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 3. 创建HBase维表（支付类型字典）
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +           // 字典编码
                " info ROW<dic_name string>,\n" +  // 字典名称（HBase的列族结构）
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));

        // 4. 过滤退款成功数据（refund_status=1602表示成功）
        Table refundPayment = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +
                        " after['order_id'] order_id," +
                        " after['sku_id'] sku_id," +
                        " after['payment_type'] payment_type," +
                        " after['callback_time'] callback_time," +
                        " after['total_amount'] total_amount," +
                        " ts_ms " +
                        " from topic_db " +
                        " where source['table']='refund_payment' " + // 退款表
                        " and `op`='r' " +                           // 读取操作
                        " and `after`['refund_status']='1602'");     // 退款成功状态码
        tableEnv.createTemporaryView("refund_payment", refundPayment);

        // 5. 过滤退单成功数据（refund_status=0705表示成功）
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        " after['order_id'] order_id," +
                        " after['sku_id'] sku_id," +
                        " after['refund_num'] refund_num " +          // 退款数量
                        " from topic_db " +
                        " where source['table']='order_refund_info' " +
                        " and `op`='r' " +
                        " and `after`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 6. 过滤退款成功订单（order_status=1006表示退款完成）
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "after['id'] id," +                          // 订单ID
                        "after['user_id'] user_id," +                // 用户ID
                        "after['province_id'] province_id " +        // 省份ID
                        "from topic_db " +
                        "where source['table']='order_info' " +
                        "and `op`='r' " +
                        "and `after`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // 7. 关联三张表生成退款明细
        Table result = tableEnv.sqlQuery(
                "select " +
                        " rp.id," +                                 // 退款ID
                        " oi.user_id," +                            // 用户ID
                        " rp.order_id," +                           // 订单ID
                        " rp.sku_id," +                             // 商品ID
                        " oi.province_id," +                        // 省份ID
                        " rp.payment_type," +                       // 支付类型编码
                        " date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(rp.callback_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " + // 格式化日期
                        " rp.callback_time," +                       // 回调时间戳
                        " ori.refund_num," +                         // 退款数量
                        " rp.total_amount," +                        // 退款金额
                        " rp.ts_ms " +                               // 处理时间
                        " from refund_payment rp " +
                        " join order_refund_info ori " +             // 关联退单表
                        " on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        " join order_info oi " +                    // 关联订单表
                        " on rp.order_id=oi.id ");

        // 8. 创建Kafka结果表（使用Upsert模式）
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+"(" +
                " id string," +
                " user_id string," +
                " order_id string," +
                " sku_id string," +
                " province_id string," +
                " payment_type_code string," +
                " date_id string," +                                // 分区字段
                " callback_time string," +
                " refund_num string," +
                " refund_amount string," +
                " ts_ms bigint ," +
                " PRIMARY KEY (id) NOT ENFORCED " +                 // 主键（用于去重）
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));

        // 9. 写入Kafka并启动任务
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        env.execute("DwdTradeRefundPaySucDetail");
    }
}