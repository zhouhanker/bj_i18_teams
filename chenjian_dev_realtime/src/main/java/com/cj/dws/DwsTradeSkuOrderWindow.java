package com.cj.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.bean.TradeSkuOrderBean;
import com.cj.constant.Constant;

import com.cj.utils.HBaseUtil;
import com.cj.utils.dataformtutil;
import com.cj.utils.finksink;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;


/**
 * @Package com.cj.realtime.dws.DwsTradeSkuOrderWindow
 * @Author chen.jian
 * @Date 2025/4/15 14:49
 * @description: sku粒度下单业务过程聚合统计
 */
public class DwsTradeSkuOrderWindow {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.seconds(3)));

//        获取数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_trade_order_detail")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//       类型转换 jsonStr->jsonObj
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
//        按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(o -> o.getString("id"));
//        去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次接收到的json对象
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            //说明没有重复  将当前接收到的这条json数据放到状态中，并注册5s后执行的定时器
                            lastJsonObjState.update(jsonObj);
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            //说明重复了   用当前数据的聚合时间和状态中的数据聚合时间进行比较，将时间大的放到状态中
                            //伪代码
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
//        指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
//        jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObject.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObject.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObject.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                        Long ts = jsonObject.getLong("ts") ;
                        TradeSkuOrderBean orderBean = TradeSkuOrderBean
                                .builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );
//        分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);

//        开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
//        聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = dataformtutil.tsToDateTime(window.getStart());
                        String edt = dataformtutil.tsToDateTime(window.getEnd());
                        String curDate = dataformtutil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
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
                        String skuId = orderBean.getSkuId();
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                        orderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                        orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                        return orderBean;
                    }
                }
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = withSkuInfoDS.map(

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
                        String spuId = orderBean.getSpuId();
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_spu_info", spuId, JSONObject.class);
                        orderBean.setSpuName(skuInfoJsonObj.getString("spu_name"));
                        return orderBean;
                    }
                }
        );

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
                        String spuId = orderBean.getTrademarkId();
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_trademark", spuId, JSONObject.class);
                        orderBean.setTrademarkName(skuInfoJsonObj.getString("tm_name"));
                        return orderBean;
                    }
                }
        );

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
                        String spuId = orderBean.getCategory3Id();
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_category3", spuId, JSONObject.class);
                        orderBean.setCategory3Name(skuInfoJsonObj.getString("name"));
                        orderBean.setCategory2Id(skuInfoJsonObj.getString("category2_id"));
                        return orderBean;
                    }
                }
        );
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
                        String spuId = orderBean.getCategory2Id();
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_category2", spuId, JSONObject.class);
                        orderBean.setCategory2Name(skuInfoJsonObj.getString("name"));
                        orderBean.setCategory1Id(skuInfoJsonObj.getString("category1_id"));
                        return orderBean;
                    }
                }
        );
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
                        String spuId = orderBean.getCategory1Id();
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_category1", spuId, JSONObject.class);
                        orderBean.setCategory1Name(skuInfoJsonObj.getString("name"));
                        return orderBean;
                    }
                }
        );

        c1Stream.print();
//        写入doris
        c1Stream
                .map(new MapFunction<TradeSkuOrderBean, String>() {
                    @Override
                    public String map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                        return JSON.toJSONString(tradeSkuOrderBean);
                    }
                })
                .sinkTo(finksink.getDorisSink("dws_trade_sku_order_window"));


        env.execute();
    }
}
