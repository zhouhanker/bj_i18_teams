package com.lxy.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.realtime.common.function.BeanToJsonStrMapFunction
 * @Author luoxinyu
 * @Date 2025/4/9 19:43
 * @description: 1
 * 将流中对象转换为json格式字符串
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}
