package com.rb.dwd;

import com.rb.utils.DwdUtils;
import com.rb.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.rb.dwd.DwdRefundSuc
 * @Author runbo.zhang
 * @Date 2025/4/12 8:51
 * @description:
 */
public class DwdRefundSuc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DwdUtils.dwdKafkaDbInit(tEnv, "log_topic_flink_online_v1_dwd");
        DwdUtils.hbaseBaseDicInit(tEnv, "dim_zrb_online_v1", "dim_base_dic");


        DwdUtils.orderInfoRefundByStatus(tEnv,"1006");
        DwdUtils.orderRefundInfoUtil(tEnv, "0705"  , "c");

        Table refundPayment = tEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "`after`['payment_type'] payment_type," +
                        "`after`['callback_time'] callback_time," +
                        "`after`['total_amount'] total_amount," +
                        "pt, " +
                        "`source`['ts_ms']  ts_ms " +
                        "from topic_db " +
                        "where `source`['table']='refund_payment' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='1602'");
        tEnv.createTemporaryView("refund_payment", refundPayment);
        Table result = tEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(FROM_UNIXTIME(CAST(rp.callback_time AS bigint)/1000),'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts_ms " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " +
                        "on rp.payment_type=dic.dic_code ");


        tEnv.executeSql("create table "+"dwd_refund_suc"+"(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts_ms string ," +
                "PRIMARY KEY (id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL("dwd_refund_suc"));
        result.executeInsert("dwd_refund_suc");

    }


}
