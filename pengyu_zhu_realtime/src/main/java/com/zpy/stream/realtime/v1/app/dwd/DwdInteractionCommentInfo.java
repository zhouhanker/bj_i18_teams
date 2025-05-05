package com.zpy.stream.realtime.v1.app.dwd;

import com.zpy.stream.realtime.v1.constant.Constant;
import com.zpy.stream.realtime.v1.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zpy.stream.realtime.v1.app.bwd.DwdInteractionCommentInfo
 * @Author pengyu_zhu
 * @Date 2025/4/11 15:50
 * @description: DwdInteractionCommentInfo
 * 提取评论操作生成评论表，并将字典表中的相关维度退化到评论表中，写出到 Kafka 对应主题.
 */

public class DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {
        // 创建Flink流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为4
        env.setParallelism(4);

        // 创建Flink表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 启用检查点机制，每5000毫秒（即5秒）进行一次检查点，检查点模式为精确一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略，在30天内最多失败3次，每次失败后等待3秒重启
        // env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        // 创建名为topic_db的表，从Kafka主题中读取数据
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  `ts_ms` bigint "  +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 用于打印topic_db表中的所有数据
        // tableEnv.executeSql("select * from topic_db").print();

        // 从topic_db表中查询数据，筛选出source表为comment_info且操作类型为'r'的数据
        // 并提取所需字段，存储在commentInfo表中
        Table commentInfo = tableEnv.sqlQuery("select  \n" +
                "    `after`['id'] as id, \n" +
                "    `after`['user_id'] as user_id, \n" +
                "    `after`['sku_id'] as sku_id, \n" +
                "    `after`['appraise'] as appraise, \n" +
                "    `after`['comment_txt'] as comment_txt, \n" +
                "    `after`['create_time'] as create_time, " +
                "     ts_ms " +
                "     from topic_db where source['table'] = 'comment_info' and op = 'r'");

        // 用于打印commentInfo表中的数据
        // commentInfo.execute().print();

        // 将commentInfo表注册为临时视图，以便后续查询使用
        tableEnv.createTemporaryView("comment_info",commentInfo);

        // 创建名为base_dic的表，从HBase中读取数据
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));

        // 用于打印base_dic表中的所有数据
        // tableEnv.executeSql("select * from base_dic").print();

        // 将comment_info表和base_dic表进行连接，通过appraise和dic_code字段关联
        // 并选取所需字段，存储在joinedTable表中
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

        // 用于打印joinedTable表中的数据
        // joinedTable.execute().print();

        // 创建一个动态表，和要写入的Kafka主题进行映射
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

        // 将joinedTable表中的数据写入到指定的Kafka主题中
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        // 执行Flink作业，作业名称为dwd_join
        env.execute("dwd_join");
    }
}