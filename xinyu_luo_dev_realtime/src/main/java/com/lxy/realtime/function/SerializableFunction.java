package com.lxy.realtime.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

/**
 * @Package com.lxy.realtime.function.SerializableFunction
 * @Author xinyu.luo
 * @Date 2025/5/4 21:07
 * @description: SerializableFunction
 */
public class SerializableFunction implements SerializableTimestampAssigner<JSONObject> {
    @Override
    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
        return jsonObj.getLong("ts_ms") * 1000;
    }
}
