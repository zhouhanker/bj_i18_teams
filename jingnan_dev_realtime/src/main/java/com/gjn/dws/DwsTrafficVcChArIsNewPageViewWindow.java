package com.gjn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gjn.base.TrafficPageViewBean;
import com.gjn.util.DateFormatUtil;
import com.gjn.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 *  @Package com.gjn.dws.DwsTrafficVcChArIsNewPageViewWindow
 *  @Author jingnan.guo
 *  @Date 2025/4/17 11:47
 * @description: 2
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static  void main(String[] args) throws Exception {

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
                .setTopics("dwd_traffic_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

//        {"common":{"ar":"3","uid":"301","os":"Android 13.0","ch":"360","is_new":"1","md":"Redmi k50","mid":"mid_273","vc":"v2.0.1","ba":"Redmi","sid":"d5c3d8a4-6d4d-4e76-92be-28240f9ae9ae"},"page":{"page_id":"cart","during_time":5434,"last_page_id":"good_detail"},"ts":1743865917542}
//        {"common":{"ar":"3","uid":"638","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"realme Neo2","mid":"mid_17","vc":"v2.1.134","ba":"realme","sid":"c6111002-3d81-4ecb-bba5-658c29d00c47"},"page":{"page_id":"payment","item":"2257","during_time":10736,"item_type":"order_id","last_page_id":"order"},"ts":1743864652487}
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

//        {"common":{"ar":"1","uid":"640","os":"Android 13.0","ch":"oppo","is_new":"0","md":"xiaomi 13 Pro ","mid":"mid_278","vc":"v2.1.134","ba":"xiaomi","sid":"8207c377-615e-4d4f-83f9-497dbc90f35b"},"page":{"page_id":"order","item":"17","during_time":7652,"item_type":"sku_ids","last_page_id":"cart"},"ts":1743866530371}
//        {"common":{"ar":"29","uid":"42","os":"Android 12.0","ch":"xiaomi","is_new":"1","md":"xiaomi 13","mid":"mid_147","vc":"v2.1.111","ba":"xiaomi","sid":"49eb15e1-2ff1-4d8e-88fd-30df3c129ae9"},"page":{"page_id":"order","item":"34","during_time":15545,"item_type":"sku_ids","last_page_id":"good_detail"},"ts":1743867019723}


        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(o -> o.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDS.map(
            new RichMapFunction<JSONObject, TrafficPageViewBean>() {
                private ValueState<String> lastVisitDateState;

                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<String> valueStateDescriptor
                            = new ValueStateDescriptor<String>("lastVisitDateState",String.class);
                    valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                    lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                    //从状态中获取当前设置上次访问日期
                    String lastVisitDate = lastVisitDateState.value();
                    //获取当前访问日期
                    Long ts = jsonObj.getLong("ts");
                    String curVisitDate = DateFormatUtil.tsToDate(ts);
                    Long uvCt = 0L;
                    if(StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)){
                        uvCt = 1L;
                        lastVisitDateState.update(curVisitDate);
                    }

                    String lastPageId = pageJsonObj.getString("last_page_id");

                    Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L ;

                    return new TrafficPageViewBean(
                            "",
                            "",
                            "",
                            commonJsonObj.getString("vc"),
                            commonJsonObj.getString("ch"),
                            commonJsonObj.getString("ar"),
                            commonJsonObj.getString("is_new"),
                            uvCt,
                            svCt,
                            1L,
                            pageJsonObj.getLong("during_time"),
                            ts
                    );
                }
            }
        );
//        TrafficPageViewBean(stt=, edt=, cur_date=, vc=v2.1.134, ch=xiaomi, ar=3, isNew=0, uvCt=0, svCt=0, pvCt=1, durSum=10736, ts=1743864652487)
//        TrafficPageViewBean(stt=, edt=, cur_date=, vc=v2.1.134, ch=oppo, ar=1, isNew=0, uvCt=0, svCt=0, pvCt=1, durSum=7652, ts=1743866530371)

        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean bean, long l) {
                                return bean.getTs();
                            }
                        }
                )
        );

        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyDS = withWatermarkDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                return Tuple4.of(
                        bean.getVc(),
                        bean.getCh(),
                        bean.getAr(),
                        bean.getIsNew()
                );
            }
        });
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS = dimKeyDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {

                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean pageViewBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        pageViewBean.setStt(stt);
                        pageViewBean.setEdt(edt);
                        pageViewBean.setCur_date(curDate);
                        out.collect(pageViewBean);
                    }
                }
        );
//        reduceDS.print();


        reduceDS
                .map(new MapFunction<TrafficPageViewBean, String>() {
                    @Override
                    public String map(TrafficPageViewBean userLoginBean) throws Exception {
                        return JSON.toJSONString(userLoginBean);
                    }
                })
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
        env.execute();
    }
}
