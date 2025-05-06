package com.lzr.conf.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.lzr.retail.com.lzy.stream.realtime.v1.realtime.bean.DimJoinFunction
 * @Author lv.zirao
 * @Date 2025/4/8 8:46
 * @description: DimJoinFunction
 */

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
