package com.sdy.dwd;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.sdy.retail.v1.realtime.dwd.Dwdbf
 * @Author danyu-shi
 * @Date 2025/4/11 20:52
 * @description:
 * 加购事实表
 */
public class Dwdbf {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "    `before` MAP<STRING, STRING>,\n" +
                "    `after` MAP<STRING, STRING>,\n" +
                "    `source` MAP<STRING, STRING>,\n" +
                "    `op` STRING,\n" +
                "    `ts_ms` STRING,\n" +
                "    proc_time AS proctime()\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'stream-dev2-danyushi',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from topic_db").print();

        Table cartInfo = tableEnv.sqlQuery("select\n" +
                "`after`['id'] as id,\n" +
                "`after`['user_id'] as user_id,\n" +
                "`after`['sku_id'] as sku_id,\n" +
                "if(op = 'i',`after`['sku_num'], CAST((CAST(`after`['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
                "ts_ms\n" +
                "from topic_db WHERE `source`['table'] = 'cart_info' \n" +
                "and \n" +
                "(op='i' or op = 'u' and `before`['sku_num']is not null and (CAST(`after`['sku_num'] AS INT) > CAST(`before`['sku_num'] AS INT)));\n");


//        cartInfo.execute().print();

        tableEnv.executeSql("CREATE TABLE stream_DwdcartTable_danyushi(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_num string,\n" +
                "ts_ms string,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ")WITH(\n" +
                "'connector' = 'upsert-kafka',\n" +
                "'topic' = 'stream_DwdcartTable_danyushi',\n" +
                "'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "'key.format' = 'json',\n" +
                "'value.format' = 'json'\n" +
                ");");
        cartInfo.executeInsert("stream_DwdcartTable_danyushi");



    }
}
