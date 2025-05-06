package com.hwq.dwd.new_old_xf;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @Package com.hwq.dwd.new_old_xf.new_to_old
 * @Author hu.wen.qi
 * @Date 2025/4/10 20:28
 * @description: 1
 */
public class new_to_old {
    @SuppressWarnings("deprecation")
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds1 = KafkaUtil.getKafkaSource(env, "start_log", "ae");
        //ds1.print();

        SingleOutputStreamOperator<String> ds2 = ds1.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) {
                boolean b = JSON.isValid(s);
                if (!b) {
                    return false;
                }
                JSONObject all = JSON.parseObject(s);
                String s1 = all.getJSONObject("common").getString("mid");
                return s1 != null;
            }
        });

        SingleOutputStreamOperator<String> new_old = ds2.keyBy(o -> JSON.parseObject(o).getJSONObject("common").getString("mid"))
                .process(new ProcessFunction<String, String>() {
                    ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> value = new ValueStateDescriptor<>("state", String.class);
                        state = getRuntimeContext().getState(value);
                    }

                    @Override
                    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        JSONObject common = jsonObject.getJSONObject("common");
                        String is_new = common.getString("is_new");

                        Long ts = jsonObject.getLong("ts");
                        String today = DateUtil.format(new Date(ts), "yyyy-mm-dd");
                        String yesterday = DateUtil.format(DateUtil.offsetDay(new Date(ts), -1), "yyyy-mm-dd");

                        String s1 = state.value();

                        if (StringUtils.isEmpty(is_new) && "1".equals(is_new)) {
                            if ("1".equals(s1)) {
                                state.update(today);
                            } else if (!"today".equals(s1)) {
                                common.put("is_new", "0");
                            }
                        } else {
                            if (StringUtils.isEmpty(s1)) {
                                state.update(yesterday);
                            }
                        }
                        collector.collect(s);
                    }
                });

        new_old.print();

        //new_old.addSink(KafkaUtil.getKafkaSink("new_old_log"));


        env.execute();
    }
}
