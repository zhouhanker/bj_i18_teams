package com.lyx.stream.realtime.v2.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.lyx.stream.realtime.v2.app.bean.TradeProvinceOrderBean;
import com.lyx.stream.realtime.v2.app.constant.Constant;
import com.lyx.stream.realtime.v2.app.function.BeanToJsonStrMapFunction;
import com.lyx.stream.realtime.v2.app.utils.DateFormatUtil;
import com.lyx.stream.realtime.v2.app.utils.FlinkSinkUtil;
import com.lyx.stream.realtime.v2.app.utils.FlinkSourceUtil;
import com.lyx.stream.realtime.v2.app.utils.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;

/**
 * @Package com.lyx.stream.realtime.v2.app.dws.DwsTradeProvinceOrderWindow
 * @Author yuxin_li
 * @Date 2025/4/21 14:56
 * @description: DwsTradeProvinceOrderWindow
 * 从 Kafka 读取订单明细数据
 * 经过去重、时间戳处理、窗口聚合、关联 HBase 数据等操作，最终将处理结果写入到 Doris 数据库
 */

public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_order_detail_yuxin_li", "dws_trade_province_order_window");

        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
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

        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) {
                        String provinceId = jsonObj.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts_ms");
                        String orderId = jsonObj.getString("order_id");
                        return TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount)
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                               // .ts_ms(ts)
                                .build();
                    }
                }
        );

//        beanDS.print();

        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        long startTs = window.getStart() / 1000;
                        long endTs = window.getEnd() / 1000;
                        String stt = DateFormatUtil.tsToDateTime(startTs);
                        String edt = DateFormatUtil.tsToDateTime(endTs);
                        String curDate = DateFormatUtil.tsToDate(startTs);
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );

//        reduceDS.print();

        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = reduceDS.map(

                new RichMapFunction<TradeProvinceOrderBean, TradeProvinceOrderBean>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeProvinceOrderBean map(TradeProvinceOrderBean orderBean) throws Exception {
                        String spuId = orderBean.getProvinceId();
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_province", spuId, JSONObject.class);
                        orderBean.setProvinceName(skuInfoJsonObj.getString("name"));
                        return orderBean;
                    }
                }
        ).setParallelism(1);

        SingleOutputStreamOperator<String> sink = withProvinceDS.map(new BeanToJsonStrMapFunction<>());
        sink.print();

        sink.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));

        env.execute("DwsTradeProvinceOrderWindow");
    }
}
