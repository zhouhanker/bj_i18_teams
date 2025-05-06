package com.zzw.app.dwd;

import com.zzw.constant.Constant;
import com.zzw.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.lzy.stream.realtime.v1.app.bwd.DwdInteractionCommentInfo
 * @Author zhengwei_zhou
 * @Date 2025/4/11 15:50
 * @description: DwdInteractionCommentInfo
 */

public class DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {
        // 获取Flink的流式执行环境，这是执行Flink流处理作业的基础
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置作业的并行度为4，即作业在运行时算子会以4个并行实例执行
        env.setParallelism(4);

        // 创建StreamTableEnvironment，用于在流处理环境中进行表和SQL相关操作
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 启用检查点机制，每隔5000毫秒（5秒）生成一个检查点
        // 采用EXACTLY_ONCE模式，确保数据处理的准确性，保证每条数据仅被处理一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 以下代码被注释，原本功能是设置重启策略，失败3次内，30天内，每次失败间隔3秒尝试重启
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        // 使用SQL语句在StreamTableEnvironment中创建一个名为topic_db的表
        // 表结构包含after（键值对映射类型）、source（键值对映射类型）、op（字符串类型）、ts_ms（长整型）字段
        // 并通过自定义工具类SQLUtil获取Kafka相关的DDL配置，指定从Constant.TOPIC_DB主题读取数据，
        // 处理后写入Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO主题
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  `ts_ms` bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 以下代码被注释，原本功能是查询topic_db表并打印结果，用于调试
//        tableEnv.executeSql("select * from topic_db").print();

        // 从topic_db表中查询符合条件的数据（source['table'] = 'comment_info'且op = 'r'）
        // 并选取after字段中的部分键值对作为新的列，如id、user_id、sku_id等，同时选取ts_ms字段
        Table commentInfo = tableEnv.sqlQuery("select  \n" +
                "    `after`['id'] as id, \n" +
                "    `after`['user_id'] as user_id, \n" +
                "    `after`['sku_id'] as sku_id, \n" +
                "    `after`['appraise'] as appraise, \n" +
                "    `after`['comment_txt'] as comment_txt, \n" +
                "    `after`['create_time'] as create_time, " +
                "     ts_ms " +
                "     from topic_db where source['table'] = 'comment_info' and op = 'r'");
        // 以下代码被注释，原本功能是执行查询并打印结果，用于调试
//        commentInfo.execute().print();

        // 在StreamTableEnvironment中创建一个临时视图comment_info，基于前面查询得到的Table对象
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // 使用SQL语句创建一个名为base_dic的表，表结构包含dic_code（字符串类型）、
        // info（包含dic_name字符串的行类型）字段，并指定主键（但不强制约束）
        // 通过自定义工具类SQLUtil获取HBase相关的DDL配置，用于从HBase读取数据
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));
        // 以0下代码被注释，原本功能是查询base_dic表并打印结果，用于调试
//        tableEnv.executeSql("select * from base_dic").print();

        // 对临时视图comment_info和表base_dic进行JOIN操作，基于c.appraise = dic.dic_code条件
        // 选取相关字段组成新的结果集，如id、user_id、sku_id等，同时将appraise对应的dic_name作为新列appraise_name
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
        // 以下代码被注释，原本功能是执行查询并打印结果，用于调试
//        joinedTable.execute().print();

        // 使用SQL语句创建一个与Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO主题映射的动态表
        // 表结构包含id、user_id、sku_id等字段，并指定主键（但不强制约束）
        // 通过自定义工具类SQLUtil获取Kafka Upsert相关的DDL配置，用于将数据写入Kafka主题
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
        // 将前面JOIN操作得到的结果集写入到创建的与Kafka主题映射的动态表中，
        // 实际会将数据写入对应的Kafka主题Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        // 执行Flink作业，作业名称为"dwd_join"
        env.execute("dwd_join");
    }





//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(4);
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//
////        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//
//        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
//                "  after MAP<string, string>, \n" +
//                "  source MAP<string, string>, \n" +
//                "  `op` string, \n" +
//                "  `ts_ms` bigint " +
//                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
////        tableEnv.executeSql("select * from topic_db").print();
//
//        Table commentInfo = tableEnv.sqlQuery("select  \n" +
//                "    `after`['id'] as id, \n" +
//                "    `after`['user_id'] as user_id, \n" +
//                "    `after`['sku_id'] as sku_id, \n" +
//                "    `after`['appraise'] as appraise, \n" +
//                "    `after`['comment_txt'] as comment_txt, \n" +
//                "    `after`['create_time'] as create_time, " +
//                "     ts_ms " +
//                "     from topic_db where source['table'] = 'comment_info' and op = 'r'");
////        commentInfo.execute().print();
//
//        tableEnv.createTemporaryView("comment_info",commentInfo);
//
//
//        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
//                " dic_code string,\n" +
//                " info ROW<dic_name string>,\n" +
//                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
//                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));
////        tableEnv.executeSql("select * from base_dic").print();
//
//        Table joinedTable = tableEnv.sqlQuery("SELECT  \n" +
//                "    id,\n" +
//                "    user_id,\n" +
//                "    sku_id,\n" +
//                "    appraise,\n" +
//                "    dic.dic_name appraise_name,\n" +
//                "    comment_txt,\n" +
//                "    ts_ms \n" +
//                "    FROM comment_info AS c\n" +
//                "    JOIN base_dic AS dic\n" +
//                "    ON c.appraise = dic.dic_code");
////        joinedTable.execute().print();
//
//        //创建动态表和要写入的主题进行映射
//        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
//                "    id string,\n" +
//                "    user_id string,\n" +
//                "    sku_id string,\n" +
//                "    appraise string,\n" +
//                "    appraise_name string,\n" +
//                "    comment_txt string,\n" +
//                "    ts_ms bigint,\n" +
//                "    PRIMARY KEY (id) NOT ENFORCED \n" +
//                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        // 写入
//        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
//
//        env.execute("dwd_join");
//    }
}
