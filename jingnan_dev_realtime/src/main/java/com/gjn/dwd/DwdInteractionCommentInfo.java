package com.gjn.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.gjn.dwd.DwdInteractionCommentInfo
 * @Author jingnan.guo
 * @Date 2025/4/5 10:57
 * @description:
 */
public class DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  before MAP<string,string> ,\n" +
                "  proc_time  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'stream_realtime_dev1',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'my_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//

        Table table = tenv.sqlQuery("select * from db ");
//        tenv.toChangelogStream(table).print();

        Table table1 = tenv.sqlQuery("select " +
                "after['id'] as id," +
                "after['user_id'] as user_id," +
                "after['sku_id'] as sku_id," +
                "after['appraise'] as appraise," +
                "after['comment_txt'] as comment_txt," +
                "ts_ms as ts," +
                "proc_time " +
                "from db where source['table'] = 'comment_info'");
//        tenv.toChangelogStream(table1).print();

        tenv.createTemporaryView("comment_info",table1);

        tenv.executeSql("CREATE TABLE hbase (\n" +
                " dic_code String,\n" +
                " info ROW<dic_name String>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'ns_jingnan_guo:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181'\n" +
                ");");

        Table table2 = tenv.sqlQuery("select * from hbase");
//        tenv.toChangelogStream(table2).print();

        Table table3 = tenv.sqlQuery("SELECT  " +
                " id,user_id,sku_id,appraise,dic.dic_name,comment_txt,ts \n" +
                "FROM comment_info AS c \n" +
                "  left join hbase as dic \n" +
                "    ON c.appraise = dic.dic_code;");
        table3.execute().print();


        tenv.executeSql("CREATE TABLE dwd_interaction_comment_info (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  appraise STRING,\n" +
                "  appraise_name STRING,\n" +
                "  comment_txt STRING,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_interaction_comment_info',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        table3.executeInsert("dwd_interaction_comment_info");

        env.execute();
    }

}
