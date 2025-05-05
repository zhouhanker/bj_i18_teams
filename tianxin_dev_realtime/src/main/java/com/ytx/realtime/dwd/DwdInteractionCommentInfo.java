package com.ytx.realtime.dwd;


import com.ytx.base.BaseSQLApp;
import com.ytx.constant.Constant;
import com.ytx.util.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/*
互动域评论事务事实表
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {

    public static void main(String[] args) throws Exception {
    new DwdInteractionCommentInfo().start(10012,4, Constant.TOPIC_DB);

    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {

               tableEnv.executeSql("CREATE TABLE topic_db_yue (\n" +
                "  `op` string,\n" +
                 "  `before` MAP<string,string>,\n" +
                " `after` MAP<string,string>,\n" +
                "  `source` MAP<string,string>,\n" +
                " `ts_ms` bigint,\n" +
                "`pt` as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'tianxin_yueyw',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'my_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//           tableEnv.executeSql("select * from topic_db_yue").print();

        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "             `after`['id'] id,\n" +
                "             `after`['user_id'] user_id ,\n" +
                "             `after`['sku_id'] sku_id ,\n" +
                "              `after`['appraise'] appraise,\n" +
                "              `after`['comment_txt'] comment_txt,\n" +
                "               `ts_ms`,\n" +
                "                `pt`\n" +
                "from topic_db_yue where source['table']='comment_info'");
//        commentInfo.execute().print();
        tableEnv.createTemporaryView("comment_info",commentInfo);
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+ Sqlutil.getHBaseDDL("dim_base_dic"));
//       tableEnv.executeSql("select * from base_dic").print();
//       env.execute("po");
//        评论表字典表关联
        Table joinTable = tableEnv.sqlQuery("SELECT id,user_id,\n" +
                "sku_id,appraise,dic.dic_name  appraise_name,\n" +
                "comment_txt,ts_ms\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                "    ON c.appraise = dic.dic_code");
        joinTable.execute().print();
//        写到kafka
  tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+"(\n" +
                " id STRING,\n" +
                "  user_id STRING,\n" +
                "sku_id STRING,\n" +
                "appraise STRING,\n" +
                "appraise_name STRING,\n" +
                "comment_txt STRING,\n" +
                "ts_ms bigint,\n" +
               "  PRIMARY KEY (id) NOT ENFORCED\n" +
                " )"+Sqlutil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//       joinTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);


//    env.execute();
    }
}
