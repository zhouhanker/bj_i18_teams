package com.common.base;


import com.common.utils.FlinkSourceUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/*
模版方法设计模式：在父类中定义完成某一个功能的核心算法骨架（步骤），有些步骤无法在父类中实现，需要延迟到子类中去完成
优点：约定了模版
    在不改变父类核心算法的骨架前提下，每个子类都可以有自己不同的实现
 */
public abstract class BaseApp {
    @SneakyThrows
    public void start(int port, int parallelism, String chAndGroupId, String topic,String sourceName){
        //TODO 1.基本环境准备
        //1.1制定流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //1.2设置并行度
        env.setParallelism(parallelism);
        //TODO 2.检查点相关设置
        //2.1开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //2.3设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6设置状态后端以及检查点存储路径
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/2207a/"+chAndGroupId);
        //2.7设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //TODO 3.从kafka的topic_db主题中读取业务数据
        //3.1声明消费的主题以及消费者组
        //3.2创建消费对象
        KafkaSource<String> kafkaSource1 = FlinkSourceUtil.getKafkaSource(topic, chAndGroupId);

        //3.3消费数据 封装为流
        DataStreamSource<String> kafkaSourceDS
                = env.fromSource(kafkaSource1, WatermarkStrategy.noWatermarks(), sourceName);
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaSource1, WatermarkStrategy.noWatermarks(), sourceName);

        //TODO 4.处理逻辑
        Handle(env,kafkaSourceDS);
        //TODO 5.提交作业
        env.execute();
    }

    public abstract void Handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceDS);
}
