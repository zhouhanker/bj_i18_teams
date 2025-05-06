package com.flink.realtime.dwd.db.app;

import com.struggle.flink.realtime.common.base.BaseSQLApp;
import com.struggle.flink.realtime.common.constant.Constant;
import com.struggle.flink.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ Package com.flink.realtime.dwd.db.app.DwdInteractionCommentInfo
 * @ Author  liu.bo
 * @ Date  2025/5/3 16:19
 * @ description:
 * @ version 1.0
*/
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        /*
        //开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //设置状态取消后，检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck/");
        //设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
         */
        new DwdInteractionCommentInfo().start(10012, 4);
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
        //将关联的结果写到kafka
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  appraise string,\n" +
                "  appraise_name string,\n" +
                "  comment_txt string,\n" +
                "  ts_ms bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
