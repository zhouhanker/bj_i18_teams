package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/6/02
 * 评论事实表
 */
public class DwdInteractionCommentInfo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//          检查点设置
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//
//        env.getCheckpointConfig().setCheckpointTimeout(6000L);
//
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//
//        env.setStateBackend(new HashMapStateBackend());
//
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://chd01:8020/zmkyg/");
//
//        System.setProperty("HADOOP_USER_NAME","atguigu");
        //TODO 3. 从 kafka 的 topic_db 主题中读取数据 创建动态表 ---kafka 连接器
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "    `database` string,\n" +
                "    `table` string,\n" +
                "    `type` string,\n" +
                "    `ts` bigint,\n" +
                "    `data` MAP<string, string>,\n" +
                "    `old` MAP<string, string>,\n" +
                "    proc_time as proctime()\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'topic_db',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_db").print();
//        TODO 4. 过滤出评论数据 ---where table='comment_info' type='insert'
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                " data['id'] id,\n" +
                " data['user_id'] user_id,\n" +
                " data['sku_id'] sku_id,\n" +
                " data['appraise'] appraise,\n" +
                " data['comment_txt'] comment_txt,\n" +
                " ts,\n" +
                " proc_time\n" +
                "from topic_db where `table`='comment_info' and type='insert'");
//        commentInfo.execute().print();
        //TODO 5. 从 HBase 中读取字典数据 创建动态表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "    dic_code string,\n" +
                "    info ROW<dic_name string>,\n" +
                "    PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-2.2',\n" +
                "    'table-name' = '"+Constant.HBASE_NAMESPACE+":dim_base_dic',\n" +
                "    'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181',\n" +
                "    'lookup.async' = 'true',\n" +
                "    'lookup.cache' = 'PARTIAL',\n" +
                "    'lookup.partial-cache.max-rows' = '500',\n" +
                "    'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                "    'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")");
//        tableEnv.executeSql("select * from base_dic").print();
        //TODO 6. 将评论表和字典表进行关联
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts\n" +
                "FROM comment_info AS c\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "ON c.appraise = dic.dic_code");
//        joinedTable.execute().print();
        //TODO 7. 将关联的结果写到 kafka 主题中
        //7.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE \"" + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts string\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '" + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + "',\n" +
                "    'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "    'key.format' = 'json',\n" +
                "    'value.format' = 'json'\n" +
                ")");
        //7.2 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}