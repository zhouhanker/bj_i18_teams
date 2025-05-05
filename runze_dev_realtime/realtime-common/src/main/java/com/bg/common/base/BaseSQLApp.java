package com.bg.common.base;

import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.bg.common.base.BaseSQLApp
 * @Author Chen.Run.ze
 * @Date 2025/4/11 8:35
 * @description: FlinkSQL基类
 */
public abstract class BaseSQLApp {
    public void start(int port,int parallelism,String ck) {
        //TODO 1.基本环境准备
        //1.1 指定处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(parallelism);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检食点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检食点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //2.3 设置job取消后检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置两个检查点之间最小的时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        //2.6 设置状态后端
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck" + ck);
//        env.setStateBackend(new HashMapStateBackend());
        //2.7 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME","root");
        //TODO 3.业务处理逻辑
        handle(tableEnv);
    }

    public abstract void handle(StreamTableEnvironment tableEnv);

    //从topic_db主题中读取数据,创建动态表
    public void readOdsDb(StreamTableEnvironment tableEnv,String groupId){
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `before` MAP<String,String>,\n" +
                "  `after` MAP<String,String>,\n" +
                "  `source` MAP<String,String>,\n" +
                "  `op` STRING,\n" +
                "  `ts_ms` bigint,\n" +
                "  `transaction` STRING,\n" +
                "  `pt` as proctime(),\n" +
                "  et as to_timestamp_ltz(ts_ms, 0), \n" +
                "  watermark for et as et - interval '3' second \n" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DB,groupId));
//        tableEnv.executeSql("select * from topic_db").print();
    }

    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code String,\n" +
                " info ROW<dic_name String>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+ SQLUtil.getHBaseDDL(Constant.HBASE_NAMESPACE+":dim_base_dic"));
//        tableEnv.executeSql("select * from base_dic").print();
    }
}
