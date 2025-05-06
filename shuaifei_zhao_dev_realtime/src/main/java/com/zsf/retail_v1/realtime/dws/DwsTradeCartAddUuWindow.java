package com.zsf.retail_v1.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.zsf.retail_v1.realtime.bean.CartAddUuBean;
import com.zsf.retail_v1.realtime.util.DateFormatUtil;
import com.zsf.retail_v1.realtime.util.KafkaUtil;
import lombok.SneakyThrows;
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

/**
 * &#064;Package  com.zsf.retail.v1.realtime.dws.DwsTradeCartAddUuWindow
 * &#064;Author  zhao.shuai.fei
 * &#064;Date  2025/4/15 19:00
 * &#064;description:  加购独立用户统计
 */
public class DwsTradeCartAddUuWindow {
    @SneakyThrows
    public static void main(String[] args) {
        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);

        env.enableCheckpointing(10000L);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "dwd_trade_cart_add", "dws_traffic_home_detail_page_view_window");
//        kafkaSource.print();

        //TODO 1.对流中的数据类型进行转换   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.map(JSON::parseObject);
        //{"sku_num":"1","user_id":"2893","sku_id":"7","id":"10274","ts":1718111265}
//        jsonObjDS.print();
        //TODO 2.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms");
                                    }
                                }
                        )
        );
        //TODO 3.按照用户的id进行分组
        KeyedStream<JSONObject, String> keyedDS
                = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        //TODO 4.使用Flink的状态编程  判断是否为加购独立用户  这里不需要封装统计的实体类对象，直接将jsonObj传递到下游
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
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

        cartUUDS.print();
        //TODO 5.开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS
                = cartUUDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
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
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
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
        aggregateDS.print();
//       SingleOutputStreamOperator<String> map1 = aggregateDS.map(JSON::toJSONString);
//       map1.sinkTo(SinkDoris.getDorisSink("sx_001","dws_trade_cart_add_uu_window"));
        env.execute();
    }
}
