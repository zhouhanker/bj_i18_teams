package dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import bean.TrafficHomeDetailPageViewBean;
import util.DateFormatUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.cm.dws.DwsTrafficHomeDetailPageViewWindow
 * @Author chen.ming
 * @Date 2025/4/15 15:52
 * @description:  12.3流量域首页、详情页页面浏览各窗口汇总表
 */
public class DwsTrafficHomeDetailPageViewWindow {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.1 开启检查点
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_xinyi_jiao_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        {"common":{"ar":"3","uid":"638","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"realme Neo2","mid":"mid_17","vc":"v2.1.134","ba":"realme","sid":"c6111002-3d81-4ecb-bba5-658c29d00c47"},"page":{"page_id":"payment","item":"2257","during_time":10736,"item_type":"order_id","last_page_id":"order"},"ts":1743864652487}

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
//        {"common":{"ar":"29","uid":"42","os":"Android 12.0","ch":"xiaomi","is_new":"1","md":"xiaomi 13","mid":"mid_147","vc":"v2.1.111","ba":"xiaomi","sid":"49eb15e1-2ff1-4d8e-88fd-30df3c129ae9"},"page":{"page_id":"order","item":"34","during_time":15545,"item_type":"sku_ids","last_page_id":"good_detail"},"ts":1743867019723}

        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                return "home".equals(pageId) || "good_detail".equals(pageId);

            }
        });
//        {"common":{"ar":"33","uid":"639","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14 Plus","mid":"mid_106","vc":"v2.1.134","ba":"iPhone","sid":"4e85b3cf-9c8d-48d7-9ace-77bc12557578"},"page":{"page_id":"good_detail","item":"21","during_time":13266,"item_type":"sku_id","last_page_id":"register"},"ts":1743865970843}

        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }
                )
        );
//        {"common":{"ar":"33","uid":"639","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14 Plus","mid":"mid_106","vc":"v2.1.134","ba":"iPhone","sid":"4e85b3cf-9c8d-48d7-9ace-77bc12557578"},"page":{"page_id":"good_detail","item":"21","during_time":13266,"item_type":"sku_id","last_page_id":"register"},"ts":1743865970843}

        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(o -> o.getJSONObject("common").getString("mid"));
//        {"common":{"ar":"33","uid":"639","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14 Plus","mid":"mid_106","vc":"v2.1.134","ba":"iPhone","sid":"4e85b3cf-9c8d-48d7-9ace-77bc12557578"},"page":{"page_id":"good_detail","item":"21","during_time":13266,"item_type":"sku_id","last_page_id":"register"},"ts":1743865970843}

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

            private ValueState<String> homeLastVisitDateState;

            private ValueState<String> detailLastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeValueStateDescriptor = new ValueStateDescriptor<String>("homeLastVisitDateState", String.class);
                homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                homeLastVisitDateState = getRuntimeContext().getState(homeValueStateDescriptor);

                ValueStateDescriptor<String> detailValueStateDescriptor = new ValueStateDescriptor<String>("detailLastVisitDateState", String.class);
                detailValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                detailLastVisitDateState = getRuntimeContext().getState(detailValueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                Long homeUvCt = 0L;
                Long detailUvCt = 0L;
                Long ts = jsonObject.getLong("ts");
                String curVisitDate = DateFormatUtil.tsToDate(ts);
                if ("home".equals(pageId)) {
                    String homeLastVisitDate = homeLastVisitDateState.value();
                    if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                        homeUvCt = 1L;
                        homeLastVisitDateState.update(curVisitDate);
                    }
                } else {
                    String detailLastVisitDate = detailLastVisitDateState.value();
                    if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                        detailUvCt = 1L;
                        detailLastVisitDateState.update(curVisitDate);
                    }
                }
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    out.collect(new TrafficHomeDetailPageViewBean(
                            "", "", "", homeUvCt, detailUvCt, ts
                    ));
                }
            }
        });
//        TrafficHomeDetailPageViewBean(stt=, edt=, curDate=, homeUvCt=0, goodDetailUvCt=1, ts=1743866296954)

        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        TrafficHomeDetailPageViewBean viewBean = iterable.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String CurDate = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCurDate(CurDate);
                        collector.collect(viewBean);
                    }
                }
        );
        reduceDS.print();
//        reduceDS
//                .map(new BeanToJsonStrMapFunction<TrafficHomeDetailPageViewBean>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));



        env.execute();
    }
}





























