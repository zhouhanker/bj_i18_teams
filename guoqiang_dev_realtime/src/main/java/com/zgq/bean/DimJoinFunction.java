package com.zgq.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.zgq.bean.DimJoinFunction
 * @Author  guoqiang.zhang
 * @Date  2025/5/4 13:27
 * @description: 
*/

public interface DimJoinFunction<T> {

 void addDims(T obj, JSONObject dimJsonObj) ;

 String getTableName() ;

 String getRowKey(T obj) ;
}
