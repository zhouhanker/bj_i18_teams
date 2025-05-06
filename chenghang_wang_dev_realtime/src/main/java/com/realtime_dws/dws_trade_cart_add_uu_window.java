package com.realtime_dws;

import com.Base.BaseApp;
import com.Constat.constat;
import com.bean.CartADDUU;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.utils.dataformtutil;
import com.utils.finksink;
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
 * @Package realtime_dws.dws_trade_cart_add_uu_window
 * @Author ayang
 * @Date 2025/4/16 11:58
 * @description: 加购用户数
 */
public class dws_trade_cart_add_uu_window extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dws_trade_cart_add_uu_window().start(10013, 4, "dws_trade_cart_add_uu_window", constat.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //转换类型 这个业务是干什么来着？加购表的   这个没有  只有kafka 不连维度表？
        SingleOutputStreamOperator<JSONObject> map = kafkaStrDS.map(JSON::parseObject);
//        map.print();
//        2> {"sku_num":"1","user_id":"23","sku_id":"25","id":"80","ts":1744429839605}
        //水位线

//        ts.print();
//        2> {"sku_num":"1","user_id":"29","sku_id":"12","id":"81","ts":1744429839619}
        //用户分组
        KeyedStream<JSONObject, String> keyiby = map.keyBy(o -> o.getString("user_id"));
//        user_id.print();
//        2> {"sku_num":"1","user_id":"28","sku_id":"3","id":"70","ts":1744429837877}

        //状态编程  判断是否是加购独立用户
        SingleOutputStreamOperator<JSONObject> process = keyiby.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new
                                ValueStateDescriptor<>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);

                    }
                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        String lastCartDate = lastCartDateState.value();

                        Long ts1 = jsonObject.getLong("ts");
                        String curCartDate = dataformtutil.tsToDate(ts1);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            collector.collect(jsonObject);
                            lastCartDateState.update(curCartDate);
                        }
                    }

                });
//        process.print();

//        4> {"sku_num":"1","user_id":"263","sku_id":"17","id":"1385","ts":1744419908436}
        SingleOutputStreamOperator<JSONObject> ts = process.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        })
        );
        //开窗
        AllWindowedStream<JSONObject, TimeWindow> jsonObjectTimeWindowAllWindowedStream = ts.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)));
        // 聚合
        SingleOutputStreamOperator<CartADDUU> aggregate = jsonObjectTimeWindowAllWindowedStream.aggregate(
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
                        return a+b;
                    }
                },
                new AllWindowFunction<Long, CartADDUU, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Long> iterable, Collector<CartADDUU> collector) throws Exception {
                        Long uuCt = iterable.iterator().next();
                        String stt = dataformtutil.tsToDateTime(timeWindow.getStart());
                        String edt = dataformtutil.tsToDateTime(timeWindow.getEnd());
                        String curDate = dataformtutil.tsToDate(timeWindow.getStart());
                        collector.collect(new CartADDUU(stt, edt, curDate, uuCt));
                    }
                }
        );
//        aggregate.print();
//        3> CartADDUU(stt=2025-04-16 19:14:35, edt=2025-04-16 19:14:40, curDate=2025-04-16, cartAddUuCt=3)

        //没数据
        // 写入doris

        SingleOutputStreamOperator<String> map1 = aggregate.map(new MapFunction<CartADDUU, String>() {
            @Override
            public String map(CartADDUU cartADDUU) throws Exception {
                return JSON.toJSONString(cartADDUU);
            }
        });
        map1.print();
        //        Caused by: org.apache.doris.flink.exception.DorisRuntimeException:
        //        tabel {} stream load error: realtime_v1.dws_trade_cart_add_uu_window,
        //        see more in [DATA_QUALITY_ERROR]too many filtered rows

        map1.sinkTo(finksink.getDorisSink("dws_trade_cart_add_uu_window"));

    }
}

