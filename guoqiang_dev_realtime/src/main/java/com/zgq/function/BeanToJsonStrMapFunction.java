package com.zgq.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.zgq.function.BeanToJsonStrMapFunction
 * @Author guoqiang.zhang
 * @Date 2025/5/4 13:48
 * @description:
 */

public class BeanToJsonStrMapFunction<T> implements MapFunction <T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}
