package com.hwq.dwd.flinksql;

import com.hwq.common.Constant.Constant;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package 评论事实表
 * @Author hu.wen.qi
 * @Date 2025/5/4 9:33
 * @description: 1
 */
public class Dwd_Trade_Cart_Add {
    @SneakyThrows
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

         //tEnv.executeSql("select source['table'] from topic_db").print();


        Table commentInfo = tEnv.sqlQuery
                (" select \n" +
                "    `after`['id'] id,\n" +
                "    `after`['user_id'] user_id,\n" +
                "    `after`['sku_id'] sku_id,\n" +
                "    `after`['appraise'] appraise,\n" +
                "    `after`['comment_txt'] comment_txt,\n" +
                "    ts_ms,\n" +
                "    pt\n" +
                "from topic_db where source['table'] = 'comment_info' and `op`='r'");

        //commentInfo.execute().print();

        tEnv.createTemporaryView("commont_info",commentInfo);

        tEnv.executeSql(" CREATE TABLE base_dic (\n" +
                "        dic_code String,\n" +
                "        info ROW<dic_name String>,\n" +
                "        PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                "       ) WITH (\n" +
                "        'connector' = 'hbase-2.2',\n" +
                "        'table-name' = 'dim_to_hbases:dim_base_dic',\n" +
                "        'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181',\n" +
                "        'lookup.async' = 'true',\n" +
                "        'lookup.cache' = 'PARTIAL',\n" +
                "        'lookup.partial-cache.max-rows' = '500',\n" +
                "        'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                "        'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                "       )");

        //tEnv.executeSql("select * from base_dic").print();


        Table commont_lookupjion_dic = tEnv.sqlQuery("  SELECT\n" +
                "        id,\n" +
                "        user_id,\n" +
                "        sku_id,\n" +
                "        appraise,\n" +
                "        dic.dic_name as appraise_name,\n" +
                "        comment_txt,\n" +
                "        ts_ms\n" +
                "        FROM commont_info AS c\n" +
                "        JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                "        ON c.appraise = dic.dic_code");

        commont_lookupjion_dic.execute().print();


        tEnv.executeSql("  CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +" (\n" +
                "        id String,\n" +
                "        user_id String,\n" +
                "        sku_id String,\n" +
                "        appraise String,\n" +
                "        appraise_name String,\n" +
                "        comment_txt String,\n" +
                "        ts_ms Bigint,\n" +
                "        PRIMARY KEY (id) NOT ENFORCED\n" +
                "      ) WITH (\n" +
                "        'connector' = 'upsert-kafka',\n" +
                "        'topic' = ' "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" ',\n" +
                "        'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"',\n" +
                "        'key.format' = 'json',\n" +
                "        'value.format' = 'json'\n" +
                "      )");

       //commont_lookupjion_dic.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);





    }
}
