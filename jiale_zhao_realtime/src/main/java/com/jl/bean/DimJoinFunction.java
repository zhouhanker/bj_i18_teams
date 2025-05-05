package com.jl.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.jl.DimJoinFunction
 * @Author jia.le
 * @Date 2025/4/8 8:46
 * @description: DimJoinFunction
 */

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
