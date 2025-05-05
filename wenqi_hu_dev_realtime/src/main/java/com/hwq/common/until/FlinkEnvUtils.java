package com.hwq.common.until;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.stream.common.utils.FlinkEnvUtils
 * @Author hu.wen.qi
 * @Date 2025/5/4 09:25
 * @description: Get Env
 */
public class FlinkEnvUtils {

    public static StreamExecutionEnvironment getFlinkRuntimeEnv(){
        if (CommonUtils.isIdeaEnv()){
            System.err.println("Action Local Env");
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
