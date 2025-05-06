package com.flink.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.struggle.flink.realtime.common.function.BeanToJsonStrMapFunction
 * @Author guo.jia.hui
 * @Date 2025/4/18 20:33
 * @description: 将流中对象转换为json格式字符串
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {

    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}
