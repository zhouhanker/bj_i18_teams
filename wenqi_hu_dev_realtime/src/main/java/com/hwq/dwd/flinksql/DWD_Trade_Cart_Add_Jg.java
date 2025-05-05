package com.hwq.dwd.flinksql;

import com.hwq.common.Constant.Constant;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package 加购事实表
 * @Author hu.wen.qi
 * @Date 2025/5/4 15:25
 * @description: 1
 */
public class DWD_Trade_Cart_Add_Jg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE topic_db (\n" +
                "before Map<String,String>,\n" +
                "after  Map<String,String>,\n" +
                "source Map<String,String>,\n" +
                "op      String,\n" +
                "ts_ms   BIGINT,\n" +
                "pt as proctime() \n" +
                " ) WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'log_topic',\n" +
                "'properties.bootstrap.servers' = 'cdh03:9092',\n" +
                "'properties.group.id' = 'testGroup',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'format' = 'json'\n" +
                ")");

        //tEnv.executeSql("select * from topic_db").print();


        Table cartInfo = tEnv.sqlQuery("select \n" +
                "   `after`['id'] id,\n" +
                "   `after`['user_id'] user_id,\n" +
                "   `after`['sku_id'] sku_id,\n" +
                "   if(`op`='r',`after`['sku_num'], CAST((CAST(`after`['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
                "   ts_ms\n" +
                "from topic_db \n" +
                "where `source`['table']='cart_info' \n" +
                "and (\n" +
                "    `op` = 'r'\n" +
                "    or\n" +
                "    (`op`='update' and `before`['sku_num'] is not null and (CAST(after['sku_num'] AS INT) > CAST(`before`['sku_num'] AS INT)))\n" +
                ")");

        //cartInfo.execute().print();

        tEnv.executeSql("  CREATE TABLE "+ Constant.TOPIC_DWD_TRADE_CART_ADD +" (\n" +
                "        id String,\n" +
                "        user_id String,\n" +
                "        sku_id String,\n" +
                "        sku_num String,\n" +
                "        ts_ms Bigint,\n" +
                "        PRIMARY KEY (id) NOT ENFORCED\n" +
                "      ) WITH (\n" +
                "        'connector' = 'upsert-kafka',\n" +
                "        'topic' = ' "+Constant.TOPIC_DWD_TRADE_CART_ADD+" ',\n" +
                "        'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"',\n" +
                "        'key.format' = 'json',\n" +
                "        'value.format' = 'json'\n" +
                "      )");

        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
