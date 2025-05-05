package bean;
import com.alibaba.fastjson.JSONObject;
/**
 * @Package com.cm.bean.DimJoinFunction
 * @Author chen.ming
 * @Date 2025/5/2 10:09
 * @description: 接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
