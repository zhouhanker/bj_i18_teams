package com.bg.realtime_dws.App;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.base.BaseApp;
import com.bg.common.bean.CartAddUuBean;
import com.bg.common.constant.Constant;
import com.bg.common.util.DateFormatUtil;
import com.bg.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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

/**
 * @Package com.bg.realtime_dws.App.DwsTradeCartAddUuWindow
 * @Author Chen.Run.ze
 * @Date 2025/4/16 8:37
 * @description: 加购独立用户统计
 */
public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow().start(10026, 4, "dws_trade_cart_add_uu_window", Constant.TOPIC_DWD_TRADE_CART_ADD);

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中的数据类型进行转换   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //{"sku_num":"1","user_id":"2893","sku_id":"7","id":"10274","ts":1718111265}
//        jsonObjDS.print("json-->");
        //TODO 2.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<JSONObject>) (jsonObj, recordTimestamp) -> jsonObj.getLong("ts_ms")
                        )
        );
        //TODO 3.按照用户的id进行分组
        KeyedStream<JSONObject, String> keyedDS
                = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        //{"sku_num":"1","user_id":"52","sku_id":"10","id":"13","ts_ms":1744166629477}
//        keyedDS.print("key-->");
        //TODO 4.使用Flink的状态编程  判断是否为加购独立用户  这里不需要封装统计的实体类对象，直接将jsonObj传递到下游
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        //获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts_ms") ;
                        String curCartDate = DateFormatUtil.tsToDate(ts);
//                        System.out.println(curCartDate);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );
        //{"user_id":"76","sku_id":"30","id":"144","ts_ms":1744338076756}
//        cartUUDS.print("cart-->");
        //TODO 5.开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS
                = cartUUDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1)));
        //TODO 6.聚合
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
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out){
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
        //TODO 7.将聚合的结果写到Doris
        //CartAddUuBean(stt=2025-04-16 09:46:56, edt=2025-04-16 09:46:57, curDate=2025-04-16, cartAddUuCt=12)
//        aggregateDS.print();
        SingleOutputStreamOperator<CartAddUuBean> filter = aggregateDS.filter(o -> o.getCurDate().equals("2025-05-04"));
        SingleOutputStreamOperator<String> map = filter
                .map((MapFunction<CartAddUuBean, String>) JSON::toJSONString);
        //{"cartAddUuCt":7,"curDate":"2025-04-15","edt":"2025-04-15 21:43:41","stt":"2025-04-15 21:43:40"}
        map.print("map-->");
        map.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));
    }
}
