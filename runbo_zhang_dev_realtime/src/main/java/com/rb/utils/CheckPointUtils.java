package com.rb.utils;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.rb.utils.CheckPointUtils
 * @Author runbo.zhang
 * @Date 2025/4/28 17:57
 * @description:
 */
public class CheckPointUtils {
    public static void setCk(StreamExecutionEnvironment env){
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/ck");
    }
}
