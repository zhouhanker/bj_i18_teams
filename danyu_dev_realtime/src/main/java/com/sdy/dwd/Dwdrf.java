package com.sdy.dwd;


import com.sdy.bean.KafkaUtil;import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @Package com.sdy.retail.v1.realtime.dwd.Dwdrf
 * @Author danyu-shi
 * @Date 2025/4/11 10:47
 * @description:
 * 评论事实表
 */
public class Dwdrf {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dwdRf = KafkaUtil.getKafkaSource(env, "stream-dev2-danyushi", "dwd_rf");
//        dwdRf.print();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
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


        Table commonInfo = tableEnv.sqlQuery("select\n" +
                " `after`['id'] id,\n" +
                " `after`['user_id'] user_id,\n" +
                " `after`['sku_id'] sku_id,\n" +
                " `after`['appraise'] appraise,\n" +
                " `after`['comment_txt'] comment_txt,\n" +
                " op,\n" +
                " ts_ms,\n" +
                " proc_time\n" +
                " from topic_db where `source`['table'] = 'comment_info'");


//        commonInfo.execute().print();
        tableEnv.createTemporaryView("commonInfo",commonInfo);




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

//        tableEnv.executeSql("select * from base_dic").print();

        //评论和字典关联
        Table CommdicTable = tableEnv.sqlQuery("SELECT\n" +
                "c.id,\n" +
                "c.user_id,\n" +
                "c.sku_id,\n" +
                "c.appraise,\n" +
                "dic.dic_name appraise_name,\n" +
                "c.comment_txt,\n" +
                "c.op,\n" +
                "c.ts_ms\n" +
                "FROM commonInfo AS c\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "ON c.appraise = dic.dic_code;");

        CommdicTable.execute().print();

//        tableEnv.executeSql("CREATE TABLE stream_DwdCommdicTable_danyushi(\n" +
//                "id string,\n" +
//                "user_id string,\n" +
//                "sku_id string,\n" +
//                "appraise string,\n" +
//                "appraise_name string,\n" +
//                "comment_txt string,\n" +
//                "op string,\n" +
//                "ts_ms string,\n" +
//                "PRIMARY KEY (id) NOT ENFORCED\n" +
//                ")WITH(\n" +
//                "'connector' = 'upsert-kafka',\n" +
//                "'topic' = 'stream_DwdCommdicTable_danyushi',\n" +
//                "'properties.bootstrap.servers' = 'cdh02:9092',\n" +
//                "'key.format' = 'json',\n" +
//                "'value.format' = 'json'\n" +
//                ");");
//
//
//        CommdicTable.executeInsert("stream_DwdCommdicTable_danyushi");


    }
}
