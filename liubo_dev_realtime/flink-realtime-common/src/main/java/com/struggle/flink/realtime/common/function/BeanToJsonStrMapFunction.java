package com.struggle.flink.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;
/**
 * @ version 1.0
 * @ Package com.struggle.flink.realtime.common.constant.BeanToJsonStrMapFunction
 * @ Author liu.bo
 * @ Date 2025/5/3 14:15
 * @ description: 将流中对象转换为json格式字符串
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}
