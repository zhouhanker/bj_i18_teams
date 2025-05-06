package com.lzy.app.dwd;

import com.lzy.constant.Constant;
import com.lzy.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.lzy.v1.app.bwd.DwdTradeOrderCancelDetail
 * @Author zheyuan.liu
 * @Date 2025/4/13 18:53
 * @description: DwdTradeOrderCancelDetail
 */

public class DwdTradeOrderCancelDetail {
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
