package com.zzw.app.dwd;

import com.zzw.constant.Constant;
import com.zzw.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.lzy.stream.realtime.v1.app.bwd.DwdTradeOrderCancelDetail
 * @Author zhengwei_zhou
 * @Date 2025/4/13 18:53
 * @description: DwdTradeOrderCancelDetail
 */

public class DwdTradeOrderCancelDetail {
//    public static void main(String[] args) throws Exception {
//        // 获取 Flink 的流式执行环境，用于构建和执行流处理作业
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 设置作业的并行度为 4，即每个算子将以 4 个并行实例运行
//        env.setParallelism(4);
//
//        // 创建一个流表环境，用于在 Flink 中执行 SQL 查询和操作表数据
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        // 启用检查点机制，每 5000 毫秒（即 5 秒）进行一次检查点操作
//        // 采用 EXACTLY_ONCE 模式，确保数据在处理过程中只被处理一次，保证数据的一致性
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//
//        // 设置空闲状态的保留时间为 30 分钟零 5 秒
//        // 这有助于清理长时间未使用的状态数据，节省资源
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
//
//        // 在流表环境中创建一个名为 topic_db 的表
//        // 该表的结构包含 after（键值对映射）、source（键值对映射）、op（操作类型）和 ts_ms（时间戳）字段
//        // 通过 SQLUtil 工具类获取 Kafka 的 DDL 配置，从 Constant.TOPIC_DB 主题读取数据
//        // 并将处理后的数据写入 Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO 主题
//        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
//                "  after MAP<string, string>, \n" +
//                "  source MAP<string, string>, \n" +
//                "  `op` string, \n" +
//                "  ts_ms bigint " +
//                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        // 注释掉的代码，若取消注释，可用于打印 topic_db 表的所有数据，方便调试
////        tableEnv.executeSql("select * from topic_db").print();
//
//        // 在流表环境中创建一个名为 base_dic 的表
//        // 该表包含 dic_code（字典代码）和 info（包含 dic_name 的行）字段
//        // dic_code 为主键，但不进行强制约束
//        // 通过 SQLUtil 工具类获取 HBase 的 DDL 配置，从 HBase 的 dim_base_dic 表读取数据
//        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
//                " dic_code string,\n" +
//                " info ROW<dic_name string>,\n" +
//                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
//                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
//        );
//        // 注释掉的代码，若取消注释，可用于打印 base_dic 表的所有数据，方便调试
////        tableEnv.executeSql("select * from base_dic").print();
//
//        // 从 topic_db 表中查询取消订单的数据
//        // 筛选条件为：表名为 order_info，操作类型为 'r'，订单状态为 '1001' 或 '1003'
//        // 选取订单 ID、操作时间和时间戳字段
//        Table orderCancel = tableEnv.sqlQuery("select " +
//                " `after`['id'] as id, " +
//                " `after`['operate_time'] as operate_time, " +
//                " `ts_ms` " +
//                " from topic_db " +
//                " where source['table'] = 'order_info' " +
//                " and `op` = 'r' " +
//                " and (`after`['order_status'] = '1001' " +
//                " or `after`['order_status'] = '1003')");
//        // 将查询结果创建为一个临时视图 order_cancel，方便后续查询使用
//        tableEnv.createTemporaryView("order_cancel", orderCancel);
//        // 注释掉的代码，若取消注释，可用于打印 orderCancel 表的查询结果，方便调试
////        orderCancel.execute().print();
//
//        // TODO 从下单事实表中获取下单数据
//        // 在流表环境中创建一个名为 dwd_trade_order_detail 的表
//        // 该表包含订单详细信息的各个字段
//        // 通过 SQLUtil 工具类获取 Kafka 的 DDL 配置，从 Constant.TOPIC_DWD_TRADE_ORDER_DETAIL 主题读取数据
//        // 并将处理后的数据写入 Constant.TOPIC_DWD_TRADE_ORDER_CANCEL 主题
//        tableEnv.executeSql(
//                "create table dwd_trade_order_detail(" +
//                        "  id string," +
//                        "  order_id string," +
//                        "  user_id string," +
//                        "  sku_id string," +
//                        "  sku_name string," +
//                        "  province_id string," +
//                        "  activity_id string," +
//                        "  activity_rule_id string," +
//                        "  coupon_id string," +
//                        "  date_id string," +
//                        "  create_time string," +
//                        "  sku_num string," +
//                        "  split_original_amount string," +
//                        "  split_activity_amount string," +
//                        "  split_coupon_amount string," +
//                        "  split_total_amount string," +
//                        "  ts_ms bigint " +
//                        "  )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
//
//        // TODO 将下单事实表和取消订单表进行关联
//        // 执行 SQL 查询，将 dwd_trade_order_detail 表和 order_cancel 临时视图进行关联
//        // 关联条件为订单 ID 相等
//        // 选取相关字段，包括订单详细信息和取消订单的操作时间等
//        Table result = tableEnv.sqlQuery(
//                "select  " +
//                        "  od.id," +
//                        "  od.order_id," +
//                        "  od.user_id," +
//                        "  od.sku_id," +
//                        "  od.sku_name," +
//                        "  od.province_id," +
//                        "  od.activity_id," +
//                        "  od.activity_rule_id," +
//                        "  od.coupon_id," +
//                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
//                        "  oc.operate_time," +
//                        "  od.sku_num," +
//                        "  od.split_original_amount," +
//                        "  od.split_activity_amount," +
//                        "  od.split_coupon_amount," +
//                        "  od.split_total_amount," +
//                        "  oc.ts_ms " +
//                        "  from dwd_trade_order_detail od " +
//                        "  join order_cancel oc " +
//                        "  on od.order_id = oc.id ");
//        // 注释掉的代码，若取消注释，可用于打印关联结果表的查询结果，方便调试
////        result.execute().print();
//
//        // TODO 将关联的结果写到 Kafka 主题中
//        // 在流表环境中创建一个名为 Constant.TOPIC_DWD_TRADE_ORDER_CANCEL 的表
//        // 该表包含关联结果的各个字段，id 为主键，但不进行强制约束
//        // 通过 SQLUtil 工具类获取 Kafka 的 Upsert DDL 配置，将数据写入 Constant.TOPIC_DWD_TRADE_ORDER_CANCEL 主题
//        tableEnv.executeSql(
//                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + "(" +
//                        "  id string," +
//                        "  order_id string," +
//                        "  user_id string," +
//                        "  sku_id string," +
//                        "  sku_name string," +
//                        "  province_id string," +
//                        "  activity_id string," +
//                        "  activity_rule_id string," +
//                        "  coupon_id string," +
//                        "  date_id string," +
//                        "  cancel_time string," +
//                        "  sku_num string," +
//                        "  split_original_amount string," +
//                        "  split_activity_amount string," +
//                        "  split_coupon_amount string," +
//                        "  split_total_amount string," +
//                        "  ts_ms bigint ," +
//                        "  PRIMARY KEY (id) NOT ENFORCED " +
//                        "  )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
//        // 将关联结果表的数据插入到 Constant.TOPIC_DWD_TRADE_ORDER_CANCEL 表中，即写入对应的 Kafka 主题
//        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
//
//        // 执行 Flink 作业，作业名称为 DwdTradeOrderCancelDetail
//        env.execute("DwdTradeOrderCancelDetail");
//    }




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

        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] as id, " +
                " `after`['operate_time'] as operate_time, " +
                " `ts_ms` " +
                " from topic_db " +
                " where source['table'] = 'order_info' " +
                " and `op` = 'r' " +
                " and `after`['order_status'] = '1001' " +
                " or `after`['order_status'] = '1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);
//        orderCancel.execute().print();


        //TODO 从下单事实表中获取下单数据
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

        //TODO 将下单事实表和取消订单表进行关联
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
//        result.execute().print();

        //TODO 将关联的结果写到kafka主题中
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
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

        env.execute("DwdTradeOrderCancelDetail");
    }
}
