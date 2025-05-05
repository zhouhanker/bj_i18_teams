package realtime.common.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

/**
 * @Package realtime.common.base.BaseSQLApp
 * @Author zhaohua.liu
 * @Date 2025/4/14.18:28
 * @description:
 */
public abstract class BaseSQLApp {
    public void start(int port,int parallelism,String ck) throws Exception {
        //指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //设置并行度
        env.setParallelism(parallelism);
        //指定表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //启用检查点,设置5秒,精准一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //失败率重启策略,30天内运行重启3次,间隔3秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //超时6秒创建失败
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //检查点是否保留,当作业被取消时，Flink 会保留外部检查点数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //检查点间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck/"+ck);
        //设置用户
        System.setProperty("HADOOP_USER_NAME","hdfs");

        //业务逻辑
        handle(tEnv);



    }

    //todo handle方法
    public abstract void handle(StreamTableEnvironment tEnv);

    //todo 读取kafka中数据表
    public void readOdsDb(StreamTableEnvironment tEnv,String groupId){
        tEnv.executeSql(
                "create table ods_initial(\n" +
                        " `before` MAP<String,String>," +
                        " `after` MAP<String,String>," +
                        " `source` MAP<String,String>," +
                        " `op` String," +
                        " `ts_ms` BIGINT," +//ts_ms是从cdc从mysql读取到数据的时间,因为mysql中数据出现就被读到,可以理解为事件时间
                        " pt as proctime()," +//pt是当前处理时间
                        " et as to_timestamp_ltz(ts_ms,3)," +//et将ts转为ymd格式并且作为水位线
                        " watermark for et as et - interval '3' second ) " + SQLutil.getKafkaDDL(Constant.TOPIC_ODS_INITIAL, groupId)
        );
//        tEnv.executeSql("select * from ods_initial").print();
    }

    //todo 读取hbase数据
    public void readHbaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql(
                "create table base_dic(" +
                        "`dic_code` string," +
                        "`info` row<dic_name string>," +
                        "primary key (dic_code) not enforced) "
                +SQLutil.getHbaseDDL("dim_base_dic")
        );
//        tEnv.executeSql("select * from base_dic").print();
    }

}
