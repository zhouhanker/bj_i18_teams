package com.struggle.flink.realtime.common.bean;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj);

    String getTableName();

    String getRowKey(T obj);
}
