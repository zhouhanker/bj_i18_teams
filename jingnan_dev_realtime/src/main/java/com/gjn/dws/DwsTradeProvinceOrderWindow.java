package com.gjn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gjn.base.TradeProvinceOrderBean;
import com.gjn.base.TrafficPageViewBean;
import com.gjn.constant.Constant;
import com.gjn.function.BeanToJsonStrMapFunction;
import com.gjn.util.DateFormatUtil;
import com.gjn.util.FlinkSinkUtil;
import com.gjn.util.HBaseUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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

/**
 * @Package com.gjn.dws.DwsTradeProvinceOrderWindow
 * @Author jingnan.guo
 * @Date 2025/4/17 11:47
 * @description: 2
 */
public class DwsTradeProvinceOrderWindow {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        设置了检查点的超时时间为 60000 毫秒（即 60 秒）。如果在 60 秒内检查点操作没有完成，就会被视为失败。
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        当作业被取消时，检查点数据不会被删除，而是会保留下来，这样在后续需要时可以利用这些检查点数据进行恢复操作。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        两次检查点操作之间的最小间隔时间为 2000 毫秒（即 2 秒）。这是为了避免在短时间内频繁进行检查点操作，从而影响作业的正常处理性能。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        表示在 30 天内允许的最大失败次数为 3 次。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
//        状态后端用于管理 Flink 作业的状态数据，HashMapStateBackend 会将状态数据存储在 TaskManager 的内存中，适用于小规模的状态管理。
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("dwd_trade_order_detail")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {

                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        if (s != null) {
                            collector.collect(JSON.parseObject(s));
                        }
                    }
                }
        );

//        jsonObjDS.print();
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(o -> o.getString("id"));

        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
            new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                private ValueState<JSONObject> lastJsonObjState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<JSONObject> valueStateDescriptor
                            = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                    valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                    lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                    //从状态中获取上次接收到的json对象
                    JSONObject lastJsonObj = lastJsonObjState.value();
                    if (lastJsonObj != null) {
                        //说明没有重复  将当前接收到的这条json数据放到状态中，并注册5s后执行的定时器
                        lastJsonObjState.update(jsonObj);
                        String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                        lastJsonObj.put("split_total_amount","-" + splitTotalAmount);
                        out.collect(lastJsonObj);
                    }
                    lastJsonObjState.update(jsonObj);
                    out.collect(jsonObj);
                }
            }
        );
//        distinctDS.print();

        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
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

        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("province_id");
                        BigDecimal splitOriginalAmount = jsonObject.getBigDecimal("split_total_amount");
                        Long ts = jsonObject.getLong("ts") ;
                        String orderId = jsonObject.getString("order_id");
                        TradeProvinceOrderBean orderBean = TradeProvinceOrderBean
                                .builder()
                                .provinceId(skuId)
                                .orderAmount(splitOriginalAmount)
                                .orderIdSet(Collections.singleton(orderId))
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );

//        beanDS.print();

        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);
        provinceIdKeyedDS.print();
//TODO 7.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1)));

//TODO 8.聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        // 创建一个新的可变集合，并将原集合的元素添加进去
                        java.util.Set<String> newOrderIdSet = new java.util.HashSet<>(value1.getOrderIdSet());
                        newOrderIdSet.addAll(value2.getOrderIdSet());
                        value1.setOrderIdSet(newOrderIdSet);
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
        reduceDS.print(">>>>reduce");

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
        );
        withProvinceDS.print(">>>>>>>with");

        withProvinceDS
                .map(new MapFunction<TradeProvinceOrderBean, String>() {
                    @Override
                    public String map(TradeProvinceOrderBean userLoginBean) throws Exception {
                        return JSON.toJSONString(userLoginBean);
                    }
                })
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
        env.execute();
        env.execute();
    }
}
