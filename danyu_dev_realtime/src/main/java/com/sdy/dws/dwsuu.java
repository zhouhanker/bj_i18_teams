package com.sdy.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.sdy.bean.DateFormatUtil;
import com.sdy.bean.FlinkSinkUtil;
import com.sdy.bean.KafkaUtil;
import com.sdy.dws.util.CartAddUuBean;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.sdy.retail.v1.realtime.dws.dwsuu
 * @Author danyu-shi
 * @Date 2025/4/16 15:52
 * @description:
 * 加购独立用户统计
 */
public class dwsuu {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
// 1. 解析数据, 封装 pojo 中
        DataStreamSource<String> kafkasource = KafkaUtil.getKafkaSource(env, "stream_DwdcartTable_danyushi", "dws_Home_PageView");

//        kafkasource.print();


        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkasource.map(JSON::parseObject);
        // .指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long l) {
                                return jsonObj.getLong("ts_ms");
                            }
                        }
                )
        );

//        withWatermarkDS.print();


        // 3.按照用户的id进行分组
        KeyedStream<JSONObject, String> keyedDS
                = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        // 4.使用Flink的状态编程  判断是否为加购独立用户  这里不需要封装统计的实体类对象，直接将jsonObj传递到下游
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters)  {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }
                    @SneakyThrows
                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out)  {
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        //获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts_ms");
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );

//        cartUUDS.print();

        // 5.开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS
                = cartUUDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        // 6.聚合
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out)  {
                        Long cartUUCt = values.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );
        // 7.将聚合的结果写到Doris
        aggregateDS.print();

        aggregateDS.map(new MapFunction<CartAddUuBean, String>() {
            @Override
            public String map(CartAddUuBean bean)  {
                SerializeConfig config = new SerializeConfig();
                config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
                return JSON.toJSONString(bean, config);
            }
        } )
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));




        env.execute();

    }
}
