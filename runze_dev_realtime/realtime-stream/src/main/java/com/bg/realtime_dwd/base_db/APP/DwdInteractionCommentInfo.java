package com.bg.realtime_dwd.base_db.APP;

import com.bg.common.base.BaseSQLApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.bg.realtime_dwd.base_db.app.DwdInteractionCommentInfo
 * @Author Chen.Run.ze
 * @Date 2025/4/10 19:05
 * @description: 评论事实表
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从kafka的topic_db 主题中读取数据 创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //TODO 过滤出评论数据
        Table commentInfo = tableEnv.sqlQuery( "select \n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "`after`['appraise'] appraise,\n" +
                "`after`['comment_txt'] comment_txt,\n" +
                "`ts_ms`,\n" +
                "`pt` \n" +
                "from topic_db where `source`['table'] = 'comment_info' ");
        //+----+-----+---------+--------+---------+------------------------------+----------------------+-------------------------+
        //| op |  id | user_id | sku_id |appraise |                  comment_txt |                ts_ms |                      pt |
        //+----+-----+---------+--------+---------+------------------------------+----------------------+-------------------------+
        //| +I | 177 |     159 |      4 |    1201 | 评论内容：55244883332199643... |        1745824056988 | 2025-05-02 15:11:58.936 |
        //| +I | 178 |     159 |     12 |    1201 | 评论内容：45983616186932167... |        1745824057010 | 2025-05-02 15:11:58.936 |
        //| +I | 179 |     159 |      5 |    1201 | 评论内容：32968554319656951... |        1745824057013 | 2025-05-02 15:11:58.936 |
        //| +I | 180 |     639 |     21 |    1201 | 评论内容：76789679761799957... |        1745824057162 | 2025-05-02 15:11:58.937 |
        //| +I | 192 |     633 |     28 |    1201 | 评论内容：35866356159998484... |        1745824065207 | 2025-05-02 15:11:58.973 |
//        commentInfo.execute().print();
        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info",commentInfo);

        //TODO 从HBase中读取字典数据 创建动态表
        readBaseDic(tableEnv);

        //TODO 将评论表和字典表进行关联
        Table joinedTable = tableEnv.sqlQuery("SELECT id,user_id,sku_id,appraise,dic.dic_name appraise_name,comment_txt,ts_ms FROM comment_info as c \n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF c.pt as dic\n" +
                "ON c.appraise = dic.dic_code;");

        //+----+------+---------+--------+----------+---------------+-----------------------------+----------------------+
        //| op |   id | user_id | sku_id | appraise | appraise_name |                 comment_txt |                ts_ms |
        //+----+------+---------+--------+----------+---------------+-----------------------------+----------------------+
        //| +I |  177 |     159 |      4 |     1201 |          好评 | 评论内容：55244883332199643... |        1745824056988 |
        //| +I |  178 |     159 |     12 |     1201 |          好评 | 评论内容：45983616186932167... |        1745824057010 |
        //| +I |  179 |     159 |      5 |     1201 |          好评 | 评论内容：32968554319656951... |        1745824057013 |
        //| +I |  180 |     639 |     21 |     1201 |          好评 | 评论内容：76789679761799957... |        1745824057162 |
        //| +I |  181 |     639 |     35 |     1201 |          好评 | 评论内容：38131877512152427... |        1745824057166 |
        //| +I |  182 |     639 |      7 |     1201 |          好评 | 评论内容：86492987738569757... |        1745824057169
        joinedTable.execute().print();

        //TODO 将关联的结果写到kafka 主题中
        //7.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +" (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  appraise STRING,\n" +
                "  appraise_name STRING,\n" +
                "  comment_txt STRING,\n" +
                "  ts_ms BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //7.2 写入
//        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }


}
