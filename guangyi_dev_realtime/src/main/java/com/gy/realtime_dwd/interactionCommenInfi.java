package com.gy.realtime_dwd;


import com.gy.constat.constat;
import com.gy.utils.Sqlutil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package realtime_Dwd.interactionCommenInfi
 * @Author guangyi_zhou
 * @Date 2025/4/10 21:59
 * @description: hebing表
 */
//已经跑了

public class interactionCommenInfi {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        //2.6 设置状态后端以及检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck/" + ckAndGroupId);

        //2.7 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME","hdfs");
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);
        tableenv.executeSql(
                "CREATE TABLE topic_table_v1 (\n" +
                        "`source`  MAP<string,string>,\n" +
                        "`op` string,\n" +
                        "`ts_ms` bigint,\n" +
                        "`after` MAP<string,string> ,\n" +
                        "`before` MAP<string,string> ,\n" +
                        " `proc_time`  AS proctime()\n "+
                        ") "+ Sqlutil.getKafkaDDL(constat.TOPIC_DB,"testGroup"));

//        tableenv.executeSql("select * from topic_table_v1").print();
        Table comment_info = tableenv.sqlQuery("select\n" +
                "after['id'] as  id,\n" +
                "after['user_id'] as user_id,\n" +
                "after['sku_id'] as sku_id,\n" +
                "after['appraise']  as appraise,\n" +
                "after['comment_txt'] as comment_txt,\n" +
                "ts_ms,\n" +
                "proc_time\n" +
                "from topic_table_v1 where source['table']='comment_info' and  op='r' ");

        tableenv.createTemporaryView("comment_info" ,comment_info);
//        comment_info.execute().print();
//        tableenv.executeSql("select * from comment_info").print();


        tableenv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+ Sqlutil.getHBaseDDL("dim_base_dic"));
//       tableenv.executeSql("select * from base_dic").print();

        Table joinedTable = tableenv.sqlQuery("SELECT\n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts_ms\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "    ON c.appraise = dic.dic_code");
//        joinedTable.execute().print();

        tableenv.createTemporaryView("joinedTable",joinedTable);
//        tableenv.sqlQuery("select * from joinedTable").execute().print();

        tableenv.executeSql("CREATE TABLE dwd_interaction_comment_info (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+ Sqlutil.getUpsertKafkaDDL(constat.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        joinedTable.executeInsert(constat.TOPIC_DWD_INTERACTION_COMMENT_INFO);

    }

}
