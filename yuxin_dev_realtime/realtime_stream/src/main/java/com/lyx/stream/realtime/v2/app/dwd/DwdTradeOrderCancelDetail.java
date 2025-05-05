package com.lyx.stream.realtime.v2.app.dwd;


import com.lyx.stream.realtime.v2.app.constant.Constant;
import com.lyx.stream.realtime.v2.app.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.lyx.stream.realtime.v2.app.bwd.DwdTradeOrderCancelDetail
 * @Author yuxin_li
 * @Date 2025/4/13 18:53
 * @description: DwdTradeOrderCancelDetail
 * 配置 Flink 流处理和表处理环境，并设置检查点。
 * 创建从 Kafka 读取数据的源表。
 * 创建从 HBase 读取数据的维度表。
 * 从 Kafka 源表中查询并处理数据。
 * 创建向 Kafka 写入数据的目标表。
 * 将处理后的数据插入到目标 Kafka 表中，并执行整个 Flink 任务
 */

public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为4，即使用4个并行任务处理数据
                env.setParallelism(4);
        // 创建流表环境
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 启用检查点机制，检查点间隔为5000毫秒，采用EXACTLY_ONCE语义保证数据处理的准确性
                env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置空闲状态保留时间为30分钟5秒
                tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 1. 创建Kafka源表
        // 创建一个名为topic_db的表，表结构包含after（键值对映射）、source（键值对映射）、op（操作类型）和ts_ms（时间戳）字段
        // SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)用于生成Kafka相关的DDL配置
                tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                        "  after MAP<string, string>, \n" +
                        "  source MAP<string, string>, \n" +
                        "  `op` string, \n" +
                        "  ts_ms bigint " +
                        ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 可以用于测试，打印topic_db表中的所有数据
        // tableEnv.executeSql("select * from topic_db").print();


        // 2. 创建HBase维度表
        // 创建一个名为base_dic的表，表结构包含dic_code（字典代码）和info（包含dic_name字段的行）字段
        // SQLUtil.getHBaseDDL("dim_base_dic")用于生成HBase相关的DDL配置
                tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                        " dic_code string,\n" +
                        " info ROW<dic_name string>,\n" +
                        " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                        ") " + SQLUtil.getHBaseDDL("dim_base_dic")
                );
        // 可以用于测试，打印base_dic表中的所有数据
        // tableEnv.executeSql("select * from base_dic").print();

        // 3. 从Kafka表中查询取消订单数据并创建临时视图
        // 从topic_db表中查询出订单状态为'1001'或'1003'的取消订单数据，提取相关字段
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] as id, " +
                " `after`['operate_time'] as operate_time, " +
                " `ts_ms` " +
                " from topic_db " +
                " where source['table'] = 'order_info' " +
                " and `op` = 'r' " +
                " and (`after`['order_status'] = '1001' " +
                " or `after`['order_status'] = '1003')");
        // 将查询结果创建为临时视图order_cancel，方便后续操作
                tableEnv.createTemporaryView("order_cancel", orderCancel);
        // 可以用于测试，打印order_cancel查询结果
        // orderCancel.execute().print();


        // 4. 创建下单事实表
        // 创建一个名为dwd_trade_order_detail的表，表结构包含多个订单相关字段
        // SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL)用于生成Kafka相关的DDL配置
                tableEnv.executeSql(
                        "create table dwd_trade_order_detail(" +
                                "  id string," +
                                "  order_id string," +
                                "  user_id string," +
                                "  sku_id string," +
                                "  sku_name string," +
                                "  province_id string," +
                                "  activity_id string," +
                                "  activity_rule_id string," +
                                "  coupon_id string," +
                                "  date_id string," +
                                "  create_time string," +
                                "  sku_num string," +
                                "  split_original_amount string," +
                                "  split_activity_amount string," +
                                "  split_coupon_amount string," +
                                "  split_total_amount string," +
                                "  ts_ms bigint " +
                                "  )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 5. 将下单事实表和取消订单表进行关联
        // 通过SQL查询将dwd_trade_order_detail表和order_cancel表进行关联，关联条件为od.order_id = oc.id
        // 并选择相关字段进行输出
                Table result = tableEnv.sqlQuery(
                        "select  " +
                                "  od.id," +
                                "  od.order_id," +
                                "  od.user_id," +
                                "  od.sku_id," +
                                "  od.sku_name," +
                                "  od.province_id," +
                                "  od.activity_id," +
                                "  od.activity_rule_id," +
                                "  od.coupon_id," +
                                "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                                "  oc.operate_time," +
                                "  od.sku_num," +
                                "  od.split_original_amount," +
                                "  od.split_activity_amount," +
                                "  od.split_coupon_amount," +
                                "  od.split_total_amount," +
                                "  oc.ts_ms " +
                                "  from dwd_trade_order_detail od " +
                                "  join order_cancel oc " +
                                "  on od.order_id = oc.id ");
        // 可以用于测试，打印关联结果
        // result.execute().print();

        // 6. 创建Kafka目标表并将关联结果写入
        // 创建一个名为Constant.TOPIC_DWD_TRADE_ORDER_CANCEL的表，表结构包含多个订单相关字段，并指定id为主键（不强制约束）
        // SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL)用于生成Kafka相关的DDL配置
                tableEnv.executeSql(
                        "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
                                "  id string," +
                                "  order_id string," +
                                "  user_id string," +
                                "  sku_id string," +
                                "  sku_name string," +
                                "  province_id string," +
                                "  activity_id string," +
                                "  activity_rule_id string," +
                                "  coupon_id string," +
                                "  date_id string," +
                                "  cancel_time string," +
                                "  sku_num string," +
                                "  split_original_amount string," +
                                "  split_activity_amount string," +
                                "  split_coupon_amount string," +
                                "  split_total_amount string," +
                                "  ts_ms bigint ," +
                                "  PRIMARY KEY (id) NOT ENFORCED " +
                                "  )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
        // 将关联结果result插入到Constant.TOPIC_DWD_TRADE_ORDER_CANCEL表中
                result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);


        env.execute("DwdTradeOrderCancelDetail");
    }
}
