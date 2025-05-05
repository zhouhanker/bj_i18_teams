package com.sdy.dwd;

import com.sdy.bean.KafkaUtil;import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.sdy.retail.v1.realtime.dwd.DwdTDPaySucTable
 * @Author danyu-shi
 * @Date 2025/4/14 21:56
 * @description:
 */
public class DwdTDPaySucTable {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> dwdRf = KafkaUtil.getKafkaSource(env, "stream-dev2-danyushi", "dwd_rf");
        dwdRf.print();

//        env.execute();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "    `before` MAP<STRING, STRING>,\n" +
                "    `after` MAP<STRING, STRING>,\n" +
                "    `source` MAP<STRING, STRING>,\n" +
                "    `op` STRING,\n" +
                "    `ts_ms` BIGINT,\n" +
                "    proc_time AS proctime(),\n" +
                "  et as to_timestamp_ltz(ts_ms, 3), " +
                "  watermark for et as et - interval '3' second " +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'stream-dev2-danyushi',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("select * from topic_db").print();

//字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "dic_code string,\n" +
                "info Row<dic_name string>,\n" +
                "PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector'='hbase-2.2',\n" +
                "'lookup.partial-cache.max-rows'='500',\n" +
                "'lookup.async'='true',\n" +
                "'lookup.cache'='PARTIAL',\n" +
                "'lookup.partial-cache.expire-after-access'='1 hour',\n" +
                "'lookup.partial-cache.expire-after-write'='1 hour',\n" +
                "'table-name'='ns_danyu_shi:dim_base_dic',\n" +
                "'zookeeper.quorum'='cdh02:2181'\n" +
                ");");
                tableEnv.executeSql("select * from base_dic").print();
        // 3. 过滤退款成功表数据
        Table refundPayment  = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "`after`['payment_type'] payment_type," +
                        "`after`['callback_time'] callback_time," +
                        "`after`['total_amount'] total_amount," +
                        "proc_time," +
                        "ts_ms " +
                        "from topic_db " +
                        "where `source`['table'] = 'refund_payment' " +
                        "and op= 'u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='1602' " );
        tableEnv.createTemporaryView("refund_payment", refundPayment);
        refundPayment.execute().print();


        // 4. 过滤退单表中的退单成功的数据
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "`after`['refund_num'] refund_num " +
                        "from topic_db " +
                        "where `source`['table'] ='order_refund_info' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        orderRefundInfo.execute().print();

        // 5. 过滤订单表中的退款成功的数据
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['user_id'] user_id," +
                        "`after`['province_id'] province_id " +
                        "from topic_db " +
                        "where `source`['table'] = 'order_info' " +
                        "and `op`='u' " +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);
        orderInfo.execute().print();


// 6. 4 张表的 join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts_ms " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.proc_time as dic " +
                        "on rp.payment_type=dic.dic_code ");
        result.execute().print();


//        tableEnv.executeSql("create table stream_dwdTDpaysucTable_danyushi(" +
//                 "id string," +
//                 "user_id string," +
//                 "order_id string," +
//                 "sku_id string," +
//                 "province_id string," +
//                 "payment_type_code string," +
//                 "payment_type_name string," +
//                 "date_id string," +
//                 "callback_time string," +
//                 "refund_num string," +
//                 "refund_amount string," +
//                 "ts bigint " +
//                 ")WITH(\n" +
//                "'connector' = 'upsert-kafka',\n" +
//                "'topic' = 'stream_dwdTDpaysucTable_danyushi',\n" +
//                "'properties.bootstrap.servers' = 'cdh02:9092',\n" +
//                "'key.format' = 'json',\n" +
//                "'value.format' = 'json'\n" +
//                ");");
//
//
//
//        result.executeInsert("stream_dwdTDpaysucTable_danyushi");




    }
}
