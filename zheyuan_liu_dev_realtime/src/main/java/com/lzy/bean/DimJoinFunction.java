package com.lzy.bean;


import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.lzy.retail.v1.realtime.bean.DimJoinFunction
 * @Author zheyuan.liu
 * @Date 2025/4/8 8:46
 * @description: DimJoinFunction
 */

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
