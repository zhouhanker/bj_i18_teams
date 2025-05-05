package dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import bean.TradeProvinceOrderBean;
import dws.function.CustomStringDeserializationSchema;
import function.DimAsyncFunction;
import util.DateFormatUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
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
/**
 * @Package com.cm.dws.DwsTradeProvinceOrderWindow
 * @Author chen.ming
 * @Date 2025/4/16 15:24
 * @description: 交易域省份粒度下单各窗口汇总表
 */
public class DwsTradeProvinceOrderWindow{
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        //TODO 2.检查点相关的设置
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.1 开启检查点
//        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //2.3 设置job取消后检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置两个检查点之间最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        2.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_trade_order_detail")
                .setGroupId("my-group")
                .setProperty("consumer.ignore.invalid.records", "true") // 跳过无效记录
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CustomStringDeserializationSchema())
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
        jsonObjDS.print();
        //TODO 2.按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        
        //TODO 3.去重
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
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //重复  需要对影响到度量值的字段进行取反 发送到下游
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        distinctDS.print();
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms") ;
                                    }
                                }
                        )
        );
        //TODO 5.再次对流中数据进行类型转换  jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                        //{"create_time":"2024-06-11 19:35:25","sku_num":"1","activity_rule_id":"1","split_original_amount":"6999.0000","split_coupon_amount":"0.0",
                        // "sku_id":"2","date_id":"2024-06-11","user_id":"616","province_id":"17","activity_id":"1","sku_name":"小米","id":"19772","order_id":"13959",
                        // "split_activity_amount":"500.0","split_total_amount":"6499.0","ts":1718278525}
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
                }
        );
//        beanDS.print();
        //TODO 6.分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //TODO 7.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 8.聚合
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
//        reduceDS.print();
        //TODO 9.关联省份维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setProvinceName(dimJsonObj.getString("name"));
                    }
                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean orderBean) {
                        return orderBean.getProvinceId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        withProvinceDS.print();

        //TODO 10.将关联的结果写到Doris中
//        withProvinceDS
//                .map(new BeanToJsonStrMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));

        env.execute();
    }
}
