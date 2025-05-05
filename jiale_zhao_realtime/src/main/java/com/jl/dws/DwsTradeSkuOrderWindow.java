package com.jl.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jl.bean.TradeSkuOrderBean;
import com.jl.function.BeanToJsonStrMapFunction;
import com.jl.function.DimAsyncFunction;
import com.jl.utils.DateFormatUtil;
import com.jl.utils.FlinkSinkUtil;
import com.jl.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.jl.DwsTradeSkuOrderWindow
 * @Author jia.le
 * @Date 2025/4/18 13:52
 * @description: DwsTradeSkuOrderWindow
 */

public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_order_detail", "dws_trade_sku_order_window");

        DataStreamSource<String> kafkaStrDS =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        if (value != null) {
                            JSONObject jsonObj = JSON.parseObject(value);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

//        jsonObjDS.print();

        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            lastJsonObjState.update(jsonObj);
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            String lastTs = lastJsonObj.getString("聚合时间戳");
                            String curTs = jsonObj.getString("聚合时间戳");
                            if (curTs.compareTo(lastTs) >= 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //当定时器被触发执行的时候，将状态中的数据发送到下游，并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        lastJsonObjState.clear();
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
                                        return jsonObj.getLong("ts_ms") * 1000;
                                    }
                                }
                        )
        );

//        withWatermarkDS.print();

        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) {
                        //{"create_time":"2024-06-11 10:54:40","sku_num":"1","activity_rule_id":"5","split_original_amount":"11999.0000",
                        // "split_coupon_amount":"0.0","sku_id":"19","date_id":"2024-06-11","user_id":"2998","province_id":"32",
                        // "activity_id":"4","sku_name":"TCL","id":"15183","order_id":"10788","split_activity_amount":"1199.9",
                        // "split_total_amount":"10799.1","ts":1718160880}

                        String skuId = jsonObj.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts_ms") * 1000;
                        return TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts_ms(ts)
                                .build();
                    }
                }
        );

//        beanDS.print();


        //TODO 6.分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS
                .keyBy(TradeSkuOrderBean::getSkuId);

        //TODO 7.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS
                .window(TumblingProcessingTimeWindows
                        .of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 8.聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) {
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );
//        reduceDS.print();

        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        withSkuInfoDS.print();


        //TODO 10.关联spu维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 11.关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //TODO 12.关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override

                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //TODO 13.关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //TODO 14.关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withC1DS = AsyncDataStream.unorderedWait(
                c2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

//        withC1DS.print();

        SingleOutputStreamOperator<String> jsonOrder = withC1DS.map(new BeanToJsonStrMapFunction<>());

        jsonOrder.print();

        jsonOrder.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));


        env.execute("DwsTradeSkuOrderWindow");
    }
}
