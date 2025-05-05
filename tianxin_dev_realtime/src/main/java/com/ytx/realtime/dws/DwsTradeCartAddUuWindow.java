package com.ytx.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.stream.common.utils.DateTimeUtils;
import com.ytx.base.BaseApp;
import com.ytx.bean.CartAddUuBean;
import com.ytx.constant.Constant;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
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

public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow().start(
        10026, 4, "dws_trade_cart_add_uu_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSource.map(JSON::parseObject);

//        按照用户的id进行分组
        KeyedStream<JSONObject, String> keyedDs = jsonObjDs.keyBy(jsonObject -> jsonObject.getString("user_id"));
//          判断是否为加购独立用户这里不需要封装实体类
        SingleOutputStreamOperator<JSONObject> cartDs = keyedDs.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<String> lastCartDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
//                初始化状态变量
                ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor<>("lastCartDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);

            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                总状态中获取上次加购日期
                String lastCartDate = lastCartDateState.value();
//                获取当前这次加购日期
                long ts = jsonObj.getLong("ts");
                String curCartDate = DateTimeUtils.tsToDate(ts);
                if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                    out.collect(jsonObj);
                    lastCartDateState.update(curCartDate);
                }
            }
        });

        SingleOutputStreamOperator<JSONObject> withWatermarkDS = cartDs.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );
//        withWatermarkDS.print();

//  开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDs =
                withWatermarkDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(3)));
//        聚合
        SingleOutputStreamOperator<CartAddUuBean> aggregate = windowDs.aggregate(new AggregateFunction<JSONObject, Long, Long>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }
            @Override
            public Long add(JSONObject jsonObject, Long accumullator) {
                return accumullator;
            }
            @Override
            public Long getResult(Long accumullator) {
                return accumullator;
            }
            @Override
            public Long merge(Long aLong, Long acc1) {
                return null;
            }
    },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                        Long cartUUct = values.iterator().next();
                        String stt = DateTimeUtils.tsToDate(window.getStart());
//                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateTimeUtils.tsToDate(window.getStart());
                        out.collect(new CartAddUuBean(stt, curDate, cartUUct));
                    }
                }
        );
        aggregate.print();
//        aggregate.map(new BeanToJsonStrMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

    }
}
