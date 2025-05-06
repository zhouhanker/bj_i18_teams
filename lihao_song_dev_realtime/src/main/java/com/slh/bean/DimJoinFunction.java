package com.slh.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.dpk.retail.v1.realtime.bean.DimJoinFunction
 * @Author lihao.song
 * @Date 2025/4/8 8:46
 * @description: DimJoinFunction
 */

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
