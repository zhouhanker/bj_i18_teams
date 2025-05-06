package com.struggle.flink.realtime.common.base;

import com.struggle.flink.realtime.common.constant.Constant;
import com.struggle.flink.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @version 1.0
 * @ Package com.struggle.flink.realtime.common.base.BaseSQLApp
 * @ Author liu.bo
 * @ Date 2025/5/3 14:09
 * @ description: flinksql基类
 */
public abstract class BaseSQLApp {
    public void start(int port, int parallelism) {
        //指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //1.2 设置并行度
        env.setParallelism(parallelism);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //业务处理逻辑
        handle(tableEnv);
    }

    public abstract void handle(StreamTableEnvironment tableEnv);

    //从topic_db主题中读取数据，创建动态表
    public static void readOdsDb(StreamTableEnvironment tableEnv,String groupId) {
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `before` MAP<STRING, STRING>,\n" +
                "  `after` MAP<STRING, STRING>,\n" +
                "  `source` MAP<STRING, STRING>,\n" +
                "  `op` STRING,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  `transaction` STRING,\n" +
                "  pro_time as proctime()" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, groupId));
    }
    //从hbase中读取字典数据 创建动态表
    public static void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info row<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL(Constant.HBASE_NAMESPACE + ":dim_base_dic"));
    }
}
