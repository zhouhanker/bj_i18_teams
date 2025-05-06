package com.cx.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cx.bean.TrafficHomeDetailPageViewBean;
import com.cx.function.BeanToJsonStrMapFunction;
import com.cx.utils.DateFormatUtil;
import com.cx.utils.FlinkSinkUtil;
import com.cx.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class DwsTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_traffic_page", "dws_traffic_home_detail_page_view_window");

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        //TODO 1.对流中数据类型进行转换   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //TODO 2.过滤首页以及详情页
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );
        //filterDS.print();
        //TODO 3.指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
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
        //TODO 4.按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 5.使用flink的状态编程  判断是否为首页以及详情页的独立访客   并将结果封装为统计的实体类对象
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    private ValueState<String> homeLastVisitDateState;
                    private ValueState<String> detailLastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> homeValueStateDescriptor
                                = new ValueStateDescriptor<>("homeLastVisitDateState", String.class);
                        homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        homeLastVisitDateState = getRuntimeContext().getState(homeValueStateDescriptor);

                        ValueStateDescriptor<String> detailValueStateDescriptor
                                = new ValueStateDescriptor<>("detailLastVisitDateState", String.class);
                        detailValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        detailLastVisitDateState = getRuntimeContext().getState(detailValueStateDescriptor);

                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");

                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        long homeUvCt = 0L;
                        long detailUvCt = 0L;

                        if ("home".equals(pageId)) {
                            //获取首页的上次访问日期
                            String homeLastVisitDate = homeLastVisitDateState.value();
                            if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                                homeUvCt = 1L;
                                homeLastVisitDateState.update(curVisitDate);
                            }
                        } else {
                            //获取详情页的上次访问日期
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
                }
        );
//        beanDS.print();
        //TODO 6.开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = beanDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 7.聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) {
                        TrafficHomeDetailPageViewBean viewBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCurDate(curDate);
                        out.collect(viewBean);
                    }
                }
        );

        SingleOutputStreamOperator<String> jsonMap = reduceDS
                .map(new BeanToJsonStrMapFunction<>());

        jsonMap.print();

        jsonMap.sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));

        env.execute("DwsTrafficHomeDetailPageViewWindow");

    }

}
