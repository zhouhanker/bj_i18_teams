package com.zpy.stream.realtime.v1.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.zpy.stream.realtime.v1.function.BeanToJsonStrMapFunction
 * @Author pengyu_zhu
 * @Date 2025/4/21 9:40
 * @description: BeanToJsonStrMapFunction
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}
