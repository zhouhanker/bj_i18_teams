package com.bhz.bean;


import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.bhz.bean.DimJoinFunction
 * @Author huizhong.bai
 * @Date 2025/5/2 14:36
 * @description: DimJoinFunction
 */

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
