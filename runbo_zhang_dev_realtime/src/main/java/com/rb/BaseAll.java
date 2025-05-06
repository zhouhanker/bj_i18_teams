package com.rb;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.rb.${NAME}
 * @Author runbo.zhang
 * @Date 2025/4/7 15:00
 * @description: ${description}
 */
public abstract class BaseAll {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60*1000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh02:8020/flink/stream_dev_v1/checkpoint");



    }
}