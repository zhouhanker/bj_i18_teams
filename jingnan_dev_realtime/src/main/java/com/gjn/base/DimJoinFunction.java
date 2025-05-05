package com.gjn.base;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.gjn.base.DimJoinFunction
 * @Author jingnan.guo
 * @Date 2025/4/15 11:05
 * @description: 接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
