package com.ytx.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ytx.base.BaseApp;
import com.ytx.bean.TradeSkuOrderBean;
import com.ytx.constant.Constant;
import com.ytx.function.DimAsyncFunction;
import com.ytx.util.DateFormatUtil;
import com.ytx.util.FlinkSinkUtil;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(10028,4,  "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> jsonobjDs = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                if (jsonStr != null) {
                    out.collect(JSON.parseObject(jsonStr));
                }
            }
        });
//        jsonobjDs.print();
//按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> keyedDs = jsonobjDs.keyBy(jsonObject -> jsonObject.getString("id"));
        //去重方式2：状态 + 抵消    优点：时效性好    缺点：如果出现重复，需要向下游传递3条数据(数据膨胀)
        SingleOutputStreamOperator<JSONObject> distinctDs = keyedDs.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
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
                    String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                    String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                    String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                    String splitTotalAmount = lastJsonObj.getString("split_total_amount");



                    lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                    lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                    lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                    lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                    out.collect(jsonObj);
                }
                lastJsonObjState.update(jsonObj);
                out.collect(jsonObj);
            }
        });
//        distinctDs.print();
//        指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDs.assignTimestampsAndWatermarks(
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
//        对流中数据进行类型转换
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDs = withWatermarkDS.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                String skuId = jsonObj.getString("sku_id");
                BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                long ts = jsonObj.getLong("ts_ms");
                TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                        .skuId(skuId)
                        .originalAmount(splitOriginalAmount)
                        .couponReduceAmount(splitCouponAmount)
                        .activityReduceAmount(splitActivityAmount)
                        .orderAmount(splitTotalAmount)
                        .ts(ts)
                        .build();
                return orderBean;
            }
        });
//      beanDs.print();

//    分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDs = beanDs.keyBy(TradeSkuOrderBean::getSkuId);
//        开窗
        AllWindowedStream<TradeSkuOrderBean, TimeWindow> windowDS  =
                skuIdKeyedDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1)));
//        聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDs = windowDS.reduce(new ReduceFunction<TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;

            }
            },
                new ProcessAllWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, TimeWindow>.Context ctx, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = iterable.iterator().next();
                        TimeWindow window = ctx.window();
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
//   reduceDs.print();
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDs,
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

//       withSkuInfoDS.print();
//        关联spu维度
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
//       withSpuInfoDS.print();
        // 11.关联tm维度
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
        // 12.关联category3维度
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

        //13.关联category2维度
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

        // 14.关联category1维度
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
      withC1DS.print();
//        withC1DS.map(new BeanToJsonStrMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));
    }
}
