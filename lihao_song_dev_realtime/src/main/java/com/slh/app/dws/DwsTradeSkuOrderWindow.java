package com.slh.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.bean.TradeSkuOrderBean;
import com.slh.constant.Constant;
import com.slh.function.BeanToJsonStrMapFunction;
import com.slh.utils.DateFormatUtil;
import com.slh.utils.FlinkSinkUtil;
import com.slh.utils.FlinkSourceUtil;
import com.slh.utils.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;

/**
 * @Package com.slh.app.dws.DwsTradeSkuOrderWindow
 * @Author lihao_song
 * @Date 2025/4/18 13:52
 * @description: DwsTradeSkuOrderWindow 交易库存单位订单窗口
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
                            String lastTs = lastJsonObj.getString("ts_ms");
                            String curTs = jsonObj.getString("ts_ms");
                            if (curTs.compareTo(lastTs) >= 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
//                        当定时器被触发执行的时候，将状态中的数据发送到下游，并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        lastJsonObjState.clear();
                    }
                }
        );
//
//
//        distinctDS.print("s->");

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

//        withWatermarkDS.print("a->");
//
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) {
//                        {"create_time":"2024-06-11 10:54:40","sku_num":"1","activity_rule_id":"5","split_original_amount":"11999.0000",
//                         "split_coupon_amount":"0.0","sku_id":"19","date_id":"2024-06-11","user_id":"2998","province_id":"32",
//                         "activity_id":"4","sku_name":"TCL","id":"15183","order_id":"10788","split_activity_amount":"1199.9",
//                         "split_total_amount":"10799.1","ts":1718160880}
//
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

//        beanDS.print("c->");
//

//        TODO 6.分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS
                .keyBy(TradeSkuOrderBean::getSkuId);
//
//        TODO 7.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS
                .window(TumblingProcessingTimeWindows
                        .of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
//
//        TODO 8.聚合
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
//        reduceDS.print("d->");
//
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            if (orderBean.getSkuId() != null) {  // 使用skuId而不是spuId
                                JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_sku_info", orderBean.getSkuId(), JSONObject.class);
                                if (skuInfoJsonObj != null) {
                                    orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                                    orderBean.setSpuName(skuInfoJsonObj.getString("spu_name"));
                                    orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                                    orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                                }
                            }
                        } catch (Exception e) {
//                             记录错误日志但继续处理
                            System.err.println("Error getting sku info for skuId: " + orderBean.getSkuId() + ", " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

//         修改后的withTmDS部分
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = withSpuInfoDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            String tmId = orderBean.getTrademarkId();
                            if (tmId != null) {
                                JSONObject tmJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_base_trademark", tmId, JSONObject.class);
                                if (tmJsonObj != null) {
                                    orderBean.setTrademarkName(tmJsonObj.getString("tm_name"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting trademark info: " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

//         修改后的c3Stream部分
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = withTmDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            String category3Id = orderBean.getCategory3Id();
                            if (category3Id != null) {
                                JSONObject c3JsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_base_category3", category3Id, JSONObject.class);
                                if (c3JsonObj != null) {
                                    orderBean.setCategory3Name(c3JsonObj.getString("name"));
                                    orderBean.setCategory2Id(c3JsonObj.getString("category2_id"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting category3 info: " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

//         修改后的c2Stream部分
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = c3Stream.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            String category2Id = orderBean.getCategory2Id();
                            if (category2Id != null) {
                                JSONObject c2JsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_base_category2", category2Id, JSONObject.class);
                                if (c2JsonObj != null) {
                                    orderBean.setCategory2Name(c2JsonObj.getString("name"));
                                    orderBean.setCategory1Id(c2JsonObj.getString("category1_id"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting category2 info: " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

//         修改后的c1Stream部分
        SingleOutputStreamOperator<TradeSkuOrderBean> c1Stream = c2Stream.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            String category1Id = orderBean.getCategory1Id();
                            if (category1Id != null) {
                                JSONObject c1JsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_base_category1", category1Id, JSONObject.class);
                                if (c1JsonObj != null) {
                                    orderBean.setCategory1Name(c1JsonObj.getString("name"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting category1 info: " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

        SingleOutputStreamOperator<String> jsonOrder = c1Stream.map(new BeanToJsonStrMapFunction<>());
//
        jsonOrder.print();
//
        jsonOrder.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));

        env.execute("DwsTradeSkuOrderWindow");
    }
}
