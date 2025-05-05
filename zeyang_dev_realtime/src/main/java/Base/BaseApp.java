package Base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import realtime_Dim.flinkfcation.flinksorceutil;

/**
 * @Package Base.BaseApp
 * @Author a-yang
 * @Date 2025-4-1011:49
 * @description: 基类
 */
public  abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //1.2 设置并行度
        env.setParallelism(parallelism);

        // 5000毫秒检查一次    精确一次语义。
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup
                (CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                Time.days(30),Time.seconds(3)));

        //2.6 设置状态后端以及检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck/" + ckAndGroupId);

        //2.7 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME","root");

        //TODO 3.从kafka的主题中读取业务数据
        //3.1 声明消费的主题以及消费者组
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = flinksorceutil.getkafkasorce(topic);

        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaDs
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        //TODO 4.处理逻辑
        handle(env,kafkaDs);
        //TODO 5.提交作业
        env.execute();
    }

    public abstract  void handle
            (StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);

}
