package realtime.common.base;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import realtime.common.constant.Constant;
import realtime.common.util.FlinkSinkUtil;
import realtime.common.util.FlinkSourceUtil;

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;

/**
 * @Package realtime.common.base.BaseApp
 * @Author zhaohua.liu
 * @Date 2025/4/9.9:54
 * @description: FlinkAPI应用程序的基类
 *  模板方法设计模式：
 *      在父类中定义完成某一个功能的核心算法骨架(步骤)，
 *      有些步骤在父类中没有办法实现，需要延迟到子类中去完成
 *      好处：约定了模板 在不改变父类核心算法骨架的前提下，每个子类都可以有自己不同的实现
 */
public abstract class BaseApp {
    //todo 静态代码块
    static {
        //设置hadoop用户
        System.setProperty("HADOOP_USER_NAME","hdfs");
    }



    //todo 设置流处理环境
    private static StreamExecutionEnvironment configureEnvironment(int port,int parallelism,String ckAndGroupId){
        //REST提供基于 HTTP 的接口，用于与 Flink 集群进行交互
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);

        //流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //设置并行度
        env.setParallelism(parallelism);

        //每5秒设置一次检查点
        env.enableCheckpointing(5000L);

        //设置检查点模式为精准一次
        env.getCheckpointConfig().setCheckpointingMode(EXACTLY_ONCE);
//        env.getCheckpointConfig().disableCheckpointing();
        //生成检查点超60s即为失败
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //job停止后,检查点数据任然保留
        env.getCheckpointConfig().
                setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //检查点生成完毕间隔2秒后,才可以创建下一个
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);


        //flink的重启策略
        // 固定延迟重启策略: 任务失败后,最多重启3次,每次间隔3秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        // 失败率重启策略: 在30天内,任务最多重启3次,每次间隔3秒,超过3次，就会彻底停止，避免无限期地尝试重启。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));


        //状态后端为HashMapStateBackend形式
        //状态后端负责实时管理和存储作业的状态,用于flink程序获取数据,和生成检查点
        env.setStateBackend(new HashMapStateBackend());
        //设置检查点位置为hdfs
        //当触发检查点时，HashMapStateBackend会将内存中的状态数据进行序列化，并将其发送到 HDFS 上进行存储。
        //当作业恢复时，又会从 HDFS 上读取检查点数据，并在内存中重新构建状态，以便作业能够继续从上次中断的地方恢复执行。
        //HashMapStateBackend还可以配合 Flink 的增量检查点机制，只将发生变化的状态数据发送到检查点存储，减少检查点的存储开销和恢复时间
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck/" + ckAndGroupId);


        return env;
    }




    //todo 其他需要读取kafka的分层使用
    // 参数为rest端口号,并行度个数,检查点在hdfs存储位置,读取的topic
    public void  start(int port,int parallelism,String ckAndGroupId,String topic) throws Exception {
        StreamExecutionEnvironment env = configureEnvironment(port, parallelism, ckAndGroupId);
        //读取kafka的source
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);
        //kafka数据量
        DataStreamSource<String> kafkaStrDS =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        //使用抽象方法handle处理流数据
        handle(env,kafkaStrDS);
        //提交任务
        env.execute();
    }



    //todo 重载start方法,用于ods的流环境
    public void start(int port,int parallelism,String ckAndGroupId) throws Exception {
        StreamExecutionEnvironment env = configureEnvironment(port, parallelism, ckAndGroupId);

        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("e_commerce", "*");
        DataStreamSource<String> mysqlStrDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink(Constant.TOPIC_ODS_INITIAL);
        mysqlStrDS.sinkTo(kafkaSink);
        env.execute();
    }


    //todo 抽象方法,用于在子类处理kafka读取的数据
    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> kafkaStreamDS);
}
