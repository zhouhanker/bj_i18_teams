package com.lyx.stream.realtime.v2.app.dwd;


import com.lyx.stream.realtime.v2.app.constant.Constant;
import com.lyx.stream.realtime.v2.app.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.lyx.stream.realtime.v2.app.bwd.DwdInteractionCommentInfo
 * @Author yuxin_li
 * @Date 2025/4/11 15:50
 * @description: DwdInteractionCommentInfo
 */

public class DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {
        //环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        //创建 Kafka 源表
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  `ts_ms` bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        tableEnv.executeSql("select * from topic_db").print();

        //处理 Kafka 数据
        Table commentInfo = tableEnv.sqlQuery("select  \n" +
                "    `after`['id'] as id, \n" +
                "    `after`['user_id'] as user_id, \n" +
                "    `after`['sku_id'] as sku_id, \n" +
                "    `after`['appraise'] as appraise, \n" +
                "    `after`['comment_txt'] as comment_txt, \n" +
                "    `after`['create_time'] as create_time, " +
                "     ts_ms " +
                "     from topic_db where source['table'] = 'comment_info' and op = 'r'");
//        commentInfo.execute().print();

        tableEnv.createTemporaryView("comment_info",commentInfo);

//        /
        //创建 HBase 维度表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));
//        tableEnv.executeSql("select * from base_dic").print();

        //关联数据
        Table joinedTable = tableEnv.sqlQuery("SELECT  \n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts_ms \n" +
                "    FROM comment_info AS c\n" +
                "    JOIN base_dic AS dic\n" +
                "    ON c.appraise = dic.dic_code");
//        joinedTable.execute().print();

        //创建动态表和要写入的主题进行映射创建 Kafka 目标表
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED \n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        env.execute("dwd_join");
    }
}
