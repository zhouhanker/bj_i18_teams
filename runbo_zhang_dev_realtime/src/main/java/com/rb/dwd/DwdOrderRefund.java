package com.rb.dwd;

import com.rb.utils.DwdUtils;
import com.rb.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.rb.dwd.DwdOrderRefund
 * @Author runbo.zhang
 * @Date 2025/4/11 18:42
 * @description:
 */
public class DwdOrderRefund {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DwdUtils.dwdKafkaDbInit(tEnv, "log_topic_flink_online_v1_dwd");//全表
        DwdUtils.hbaseBaseDicInit(tEnv, "dim_zrb_online_v1", "dim_base_dic");//base_dic维度表

        DwdUtils.orderInfoRefundByStatus(tEnv, "1005");
        DwdUtils.orderRefundInfoUtil(tEnv, "", "c");
        Table result = tEnv.sqlQuery(
                "select "  +
                        " ri.id," +
                        " ri.user_id," +
                        " ri.order_id," +
                        " ri.sku_id," +
                        " oi.province_id," +
                        " date_format(FROM_UNIXTIME(CAST(ri.create_time AS bigint)/1000),'yyyy-MM-dd') date_id," +
                        " ri.create_time," +
                        " ri.refund_type," +
                        " dic1.info.dic_name," +
                        " ri.refund_reason_type," +
                        " dic2.info.dic_name," +
                        " ri.refund_reason_txt," +
                        " ri.refund_num," +
                        " ri.refund_amount," +
                        " ri.ts_ms ts " +
                        " from order_refund_info ri " +
                        " join order_info oi " +
                        " on ri.order_id=oi.id " +
                        " join base_dic for system_time as of ri.pt as dic1 " +
                        " on ri.refund_type=dic1.dic_code " +
                        " join base_dic for system_time as of ri.pt as dic2 " +
                        " on ri.refund_reason_type=dic2.dic_code ");

        tEnv.executeSql(
                "create table "+"dwd_trade_order_refund"+"(" +
                        "id string," +
                                "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts string ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL("dwd_trade_order_refund"));
        result.executeInsert("dwd_trade_order_refund");
        result.execute().print();;
    }


}
