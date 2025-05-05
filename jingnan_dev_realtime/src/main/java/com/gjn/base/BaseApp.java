package com.gjn.base;

import com.gjn.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Package com.gjn.base.BaseApp
 * @Author jingnan.guo
 * @Date 2025/4/14 11:25
 * @description:FlinkAPI应用程序的基类
 */
public abstract class BaseApp {
    public void start(int port,int parallelism,String ckAndGroupId,String topic) throws Exception {

        Configuration conf =new Configuration();
        conf.setInteger("rest.port",port);

        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //设置并行度
        env.setParallelism(parallelism);
        //开启检查点
        //env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点超时时间
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //设置job取消后是否保留检查点
        //env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置两个检查点最小时间间隔
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //设置重启策咯
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        //设置状态后端及其检查点储存路径
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/2207A/stream_realtime/flink" + ckAndGroupId);
        //设置操作hadoop用户
        //System.setProperty("HADOOP_USER_NAME","jingnan_guo");

        //从kafka主题中读取业务数据

        //TODO 3.从kafka的主题中读取业务数据
        //3.1 声明消费的主题以及消费者组
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getkafkaSource(topic, ckAndGroupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        //TODO 4.处理逻辑
        handle(env,kafkaStrDS);
        //TODO 5.提交作业
        env.execute();

    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS);
}
