package com.ytx.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.ytx.base.BaseApp;
import com.ytx.bean.TradeProvinceOrderBean;
import com.ytx.bean.TradeSkuOrderBean;
import com.ytx.constant.Constant;
import com.ytx.function.DimAsyncFunction;
import com.ytx.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindow().start(
                10029,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (s != null) {
                    JSONObject jsonObj = JSON.parseObject(s);
                    out.collect(jsonObj);
                }
            }
        });
//        jsonObjDs.print();
        KeyedStream<JSONObject, String> orderKeyDs =
                jsonObjDs.keyBy(jsonObject -> jsonObject.getString("id"));

//        去重
        SingleOutputStreamOperator<JSONObject> distinctDs = orderKeyDs.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastJsonObjState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor =
                        new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastJsonObj = lastJsonObjState.value();
                if (lastJsonObj != null) {
//                     //重复  需要对影响到度量值的字段进行取反 发送到下游
                    String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                    lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                    out.collect(lastJsonObj);
                }
                lastJsonObjState.update(jsonObj);
                out.collect(jsonObj);
            }
        });
//        distinctDs.print();
//        水位线m
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts_ms");
                    }
                }));
//      再次对流中数据进行类型转换
//        {"cancel_time":"1745186736000","sku_num":"1","split_original_amount":"999.0000", "split_coupon_amount":"0.0","sku_id":"4",
//        "date_id":"2025-04-21","user_id":"1414","province_id":"18", "sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米",
//        "id":"13186","order_id":"6712", "split_activity_amount":"0.0","ts_ms":1745157510176,"split_total_amount":"999.0"}
        SingleOutputStreamOperator<TradeProvinceOrderBean> beabDs = withWatermarkDS.map(new MapFunction<JSONObject, TradeProvinceOrderBean>() {
            @Override
            public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                String provinceId = jsonObj.getString("province_id");
                BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                Long ts = jsonObj.getLong("ts_ms");
                String orderId = jsonObj.getString("order_id");

                TradeProvinceOrderBean orderBean = TradeProvinceOrderBean.builder()
                        .provinceId(provinceId)
                        .orderAmount(splitTotalAmount)
                        .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                        .ts(ts)
                        .build();
                return orderBean;
            }
        });
//      beabDs.print();
//        分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beabDs.keyBy(TradeProvinceOrderBean::getProvinceId);

        // 7.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
//      聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );
       reduceDS.print();
//        关联省份维度

       /* SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDs = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDims(TradeProvinceOrderBean obj, JSONObject dimJsonObj) {
                        obj.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean obj) {
                        return obj.getProvinceId();
                    }
                }
                , 60, TimeUnit.SECONDS
        );
        withProvinceDs.print();

        */
//        写到doris
//      withProvinceDs.map(new BeanToJsonStrMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));

    }
}
