package com.gjn.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.gjn.dwd.DwdTradeCartAdd
 * @Author jingnan.guo
 * @Date 2025/4/15 10:57
 * @description:
 */
public class DwdTradeCartAdd  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  before MAP<string,string>,\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  proc_time  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'stream_realtime_dev1',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'my_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");


        Table table = tenv.sqlQuery("select * from db");
//        tenv.toChangelogStream(table).print();

        Table table1 = tenv.sqlQuery("select " +
                "after['id'] as id," +
                "after['user_id'] as user_id," +
                "after['sku_id'] as sku_id," +
                "if(op = 'i', `after`['sku_num'], CAST((CAST(after['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING)) sku_num," +
                "ts_ms " +
                "from db " +
                "where source['table'] = 'cart_info' " +
                "and (" +
                "op = 'i' " +
                "or " +
                "op = 'u' and before['sku_num']is not null and (CAST(after['sku_num'] AS INT) > CAST(before['sku_num'] AS INT)))");
        tenv.toChangelogStream(table1).print();




        tenv.executeSql("CREATE TABLE dwd_trade_cart_add (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  sku_name STRING,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_trade_cart_add',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        table1.executeInsert("dwd_trade_cart_add");

        env.execute();
    }
}
