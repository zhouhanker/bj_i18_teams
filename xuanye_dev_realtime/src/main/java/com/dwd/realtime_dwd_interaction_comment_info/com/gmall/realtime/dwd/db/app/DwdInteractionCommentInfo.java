package com.dwd.realtime_dwd_interaction_comment_info.com.gmall.realtime.dwd.db.app;


import com.common.base.BaseSQLApp;
import com.common.constant.Constant;
import com.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;



public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdInteractionCommentInfo().start(10012,4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
//        tableEnv.executeSql("select * from KafkaTable where `source`['table']='comment_info'");
        //TODO 4.过滤出评论数据

        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id," +
                "`after`['sku_id'] sku_id,\n" +
                "`after`['appraise'] appraise,\n" +
                "`after`['comment_txt'] comment_txt,\n" +
                "ts_ms ts,\n" +
                "pt\n" +
                "from KafkaTable where `source`['table']='comment_info'");
//        commentInfo.execute().print();

        tableEnv.createTemporaryView("comment_info", commentInfo);

        readBaseDic(tableEnv);

//        tableEnv.executeSql("select * from base_dic").print();


        //TODO 6.将评论表和字典表进行关联

        Table joinedTable = tableEnv.sqlQuery("SELECT \n" +
                "c.id,\n" +
                "c.user_id,\n" +
                "c.sku_id,\n" +
                "c.appraise,\n" +
                "dic.info.dic_name as appraise_name,\n" +
                "c.comment_txt,\n" +
                "c.ts\n" +
                "FROM comment_info  AS c\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                "ON c.appraise = dic.dic_code");
        joinedTable.execute().print();



        //TODO 7.将关联的结果写到kafka主堰中
        tableEnv.executeSql("        CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +" (\n" +
                "          id STRING,\n" +
                "          user_id STRING,\n" +
                "          sku_id STRING,\n" +
                "          appraise STRING,\n" +
                "          appraise_name STRING,\n" +
                "          comment_txt STRING,\n" +
                "          ts bigint,\n" +
                "          PRIMARY KEY (id) NOT ENFORCED" +
                "          ) "+ SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

}
