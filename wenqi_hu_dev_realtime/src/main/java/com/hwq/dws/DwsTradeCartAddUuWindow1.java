package com.hwq.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.common.bean.CartAddUuBean;
import com.hwq.common.until.DateFormatUtil;
import com.hwq.common.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package 加购
 * @Author hu.wen.qi
 * @Date 2025/5/4
 * @description: 1
 *
 */
public class DwsTradeCartAddUuWindow1 {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
// 精确一次语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 检查点超时时间（2分钟）
        checkpointConfig.setCheckpointTimeout(120000);
// 最小间隔：500毫秒（防止检查点过于频繁）
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
// 最大并发检查点数
        checkpointConfig.setMaxConcurrentCheckpoints(1);


        DataStreamSource<String> cart = KafkaUtil.getKafkaSource(env, "dwd_trade_cart_add", "a1");
        //cart.print();
        SingleOutputStreamOperator<JSONObject> jsonObjDS = cart.map(JSON::parseObject);
        //jsonObjDS.print();

        //{"sku_num":"1","user_id":"2893","sku_id":"7","id":"10274","ts":1718111265}
        //jsonObjDS.print();
        //TODO 2.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms") * 1000;
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
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastCartDateState", String.class);
                       // valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        //获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts_ms") * 1000;
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );

        //cartUUDS.print();

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
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) {
                        //时间出现偏移,所以处以1000,把时间转换成毫秒
                        Long cartUUCt = values.iterator().next();
                        long l = window.getStart() / 1000;
                        String stt = DateFormatUtil.tsToDateTime(l);
                        long l1 = window.getEnd() / 1000;
                        String edt = DateFormatUtil.tsToDateTime(l1);
                        long l3 = window.getStart() / 1000;
                        String curDate = DateFormatUtil.tsToDate(l3);
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
        //aggregateDS.print();

        SingleOutputStreamOperator<String> map = aggregateDS.map(JSON::toJSONString);
        map.print();
        //map.sinkTo(SinkDoris.getDorisSink("dws_to_doris","dws_trade_cart_addUu_window1"));


        env.execute();
    }
}
