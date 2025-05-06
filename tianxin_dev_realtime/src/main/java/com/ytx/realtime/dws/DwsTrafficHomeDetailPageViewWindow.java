package com.ytx.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.stream.common.utils.DateTimeUtils;
import com.ytx.base.BaseApp;
import com.ytx.bean.TrafficHomeDetailPageViewBean;
import com.ytx.constant.Constant;
import com.ytx.util.DateFormatUtil;
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
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficHomeDetailPageViewWindow().start(10024,4,"dws_traffic_home_detail_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
//   对流中数据进行转化
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSource.map(JSON::parseObject);
//        过滤首页及详情页
        SingleOutputStreamOperator<JSONObject> filterDs = jsonObjDs.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                return "home".equals(pageId) || "good_detail".equals(pageId);
            }
        });
//        filterDs.print();
//        按照mid进行分组
        KeyedStream<JSONObject, String> keyedDs =
                filterDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
//      .使用flink的状态编程  判断是否为首页以及详情页的独立访客   并将结果封装为统计的实体类对象
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDs = keyedDs.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastVisitDateState;
            private ValueState<String> detailLastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homevalueStateDescriptor =
                        new ValueStateDescriptor<>("homeLastVisitDateState", String.class);
                homevalueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                homeLastVisitDateState = getRuntimeContext().getState(homevalueStateDescriptor);

                ValueStateDescriptor<String> detailvalueStateDescriptor =
                        new ValueStateDescriptor<>("detailLastVisitDateState", String.class);
                detailvalueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                detailLastVisitDateState = getRuntimeContext().getState(detailvalueStateDescriptor);

            }
            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = DateTimeUtils.tsToDate(ts);
                Long homeUvCt = 0L;
                Long detailUvCt = 0L;
                if ("home".equals(pageId)) {
//                  获取首页上次访问日期
                    String homeLastVisitDate = homeLastVisitDateState.value();
                    if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                        homeUvCt = 1L;
                        homeLastVisitDateState.update(curVisitDate);
                    }
                } else {
                    String detailLastVisitDate = detailLastVisitDateState.value();
                    if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDateState.equals(curVisitDate)) {
                        detailUvCt = 1L;
                        detailLastVisitDateState.update(curVisitDate);
                    }
                }
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    out.collect(new TrafficHomeDetailPageViewBean(
                            "",
                            "",
                            homeUvCt,
                            detailUvCt,
                            ts
                    ));
                }
            }
        });
//        beanDs.print();
        //水位线
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWatermarkD = beanDs.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TrafficHomeDetailPageViewBean>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficHomeDetailPageViewBean jsonObj, long l) {
                        return  jsonObj.getTs();
                    }
                }));
        //开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = withWatermarkD
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing
                        .time.Time.seconds(3)));
//        // 7.聚合
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
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean viewBean = values.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
//                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        viewBean.setStt(stt);
//                        viewBean.setEdt(edt);
                        viewBean.setCurDate(curDate);
                        out.collect(viewBean);
                    }
                }
        );
//        // 8.将聚合的结果写到Doris
        reduceDS.print();

//        reduceDS
//                .map(new BeanToJsonStrMapFunction<TrafficHomeDetailPageViewBean>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));
    }
}
