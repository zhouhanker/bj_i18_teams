package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;


/**
 * @Package com.cj.realtime.function.BeanToJsonStrMapFunction
 * @Author chen.jian
 * @Date 2025/4/15 9:29
 * @description: 转换json
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
//        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}
