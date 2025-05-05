package realtime.common.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * 维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
