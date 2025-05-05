package com.flink.realtime.dwd;

import com.flink.realtime.common.base.BaseSQLApp;
import com.flink.realtime.common.constant.Constant;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.flink.realtime.dwd.db.app.DwdInteractionCommentInfo
 * @Author guo.jia.hui
 * @Date 2025/4/13 20:32
 * @description: learn
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10015, 4);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从kafka的topic_db主题中读取数据 创建动态表 --kafka连接器
        readOdsDb(tableEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);


        //过滤出评论数据
        Table commentInfo = tableEnv.sqlQuery("select " +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "`after`['appraise'] appraise,\n" +
                "`after`['comment_txt'] comment_txt,\n" +
                "`after`['create_time'] create_time,\n" +
                "`after`['operate_time'] operate_time,\n" +
                "ts_ms,\n" +
                "pro_time " +
                "from topic_db where source['table']='comment_info' and op='c'");
        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info", commentInfo);
        //tableEnv.executeSql("select * from comment_info").print();
        //从hbase中读取字典数据 创建动态表
        readBaseDic(tableEnv);
        //tableEnv.executeSql("select * from base_dic").print();

        Table joinedTable = tableEnv.sqlQuery("select\n" +
                "\tc.id,\n" +
                "\tc.user_id,\n" +
                "\tc.sku_id,\n" +
                "\tc.appraise,\n" +
                "\tdic.dic_name as appraise_name,\n" +
                "\tc.comment_txt,\n" +
                "\tc.ts_ms\n" +
                "from comment_info as c\n" +
                "join base_dic for system_time as of c.pro_time as dic\n" +
                "on c.appraise = dic.dic_code");

        joinedTable.execute().print();
        //将关联的结果写到kafka
//        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
//                "  id string,\n" +
//                "  user_id string,\n" +
//                "  sku_id string,\n" +
//                "  appraise string,\n" +
//                "  appraise_name string,\n" +
//                "  comment_txt string,\n" +
//                "  ts_ms bigint,\n" +
//                "  PRIMARY KEY (id) NOT ENFORCED" +
//                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        //写入
//        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
