package com.rb.dws;

import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.rb.dws.Test111
 * @Author runbo.zhang
 * @Date 2025/4/21 14:29
 * @description:
 */
public class Test111 {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
//        System.setProperty("HADOOP_USER_NAME", "hdfs");

        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "log_topic_flink_online_v1_dwd");
        kafkaRead.print();

        env.disableOperatorChaining();
        env.execute();

    }
}
