package Base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.Sqlutil;

/**
 * @Package Base.BasesqlApp
 * @Author ayang
 * @Date 2025/4/11 11:25
 * @description: sql基类
 */
public abstract class BasesqlApp {
    public void start(int port,int parallelism,String ck){
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(parallelism);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        //2.1 开启检查点
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //2.3 设置状态取消后，检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略T;
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://chd01:8020/ck/" + ck);
        //2.7 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME","root");

        //TODO 3.业务处理逻辑
        handle(tableEnv);

    }

    public abstract void handle(StreamTableEnvironment tableEnv) ;
    //从topic_db主题中读取数据，创建动态表
    public void readOdsDb(StreamTableEnvironment tableEnv,String groupId) {


        tableEnv.executeSql("" +
                "CREATE TABLE topic_table_v1 (\n" +
                "  before MAP<string,string>,\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  proc_time  AS proctime(),\n "+
                "  et AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" +//2024-08-29 16:38:09.848 +00:00
                "  WATERMARK FOR et AS et - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_db").print();
    }

    //从Hbase的字典表中读取数据，创建动态表
    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + Sqlutil.getHBaseDDL("dim_base_dic"));
        //tableEnv.executeSql("select * from base_dic").print();
    }
}
