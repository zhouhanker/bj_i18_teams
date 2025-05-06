package realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import realtime.common.base.BaseApp;
import realtime.common.bean.TrafficHomeDetailPageViewBean;
import realtime.common.constant.Constant;
import realtime.common.util.DateFormatUtil;
import realtime.common.util.FlinkSinkUtil;

/**
 * @Package realtime.dws.app.DwsTrafficHomeDetailPageViewWindow
 * @Author zhaohua.liu
 * @Date 2025/4/22.21:37
 * @description:
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficHomeDetailPageViewWindow().start(
                20014,
                1,
                "dws_traffic_home_detail_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStreamDS) {
        //读取页面数据,对流进行类型转换 jsonStr->jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStreamDS.map(jsonStr -> JSON.parseObject(jsonStr));
        //过滤首页以及详情页
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );
        //指定ts为事件时间,设置为水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        )
        );
        //按mid分组
        KeyedStream<JSONObject, String> keyMidDS = withWatermarkDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //使用状态编程,判断是否为首页以及详情页的独立访客,并封装为实体类
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyMidDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                    ValueState<String> lastHomeState;
                    ValueState<String> lastDetailState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastHomeVisitDate = new ValueStateDescriptor<>("lastHomeVisitDate", String.class);
                        lastHomeVisitDate.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastHomeState = getRuntimeContext().getState(lastHomeVisitDate);

                        ValueStateDescriptor<String> lastDetailVisitDate = new ValueStateDescriptor<>("lastDetailVisitDate", String.class);
                        lastHomeVisitDate.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastDetailState = getRuntimeContext().getState(lastDetailVisitDate);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String cur_date = DateFormatUtil.tsToDate(jsonObject.getLong("ts"));
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");


                        Long homeUvCt = 0L;
                        Long detailUvCt = 0L;
                        if ("home".equals(pageId)) {
                            String lastHomeDate = lastHomeState.value();
                            if (StringUtils.isEmpty(lastHomeDate) || !lastHomeDate.equals(cur_date)) {
                                homeUvCt = 1L;
                                lastHomeState.update(cur_date);
                            }
                        } else {
                            String lastDetailDate = lastDetailState.value();
                            if (StringUtils.isEmpty(lastDetailDate) || !lastDetailDate.equals(cur_date)) {
                                detailUvCt = 1L;
                                lastDetailState.update(cur_date);
                            }
                        }

                        if (homeUvCt != 0L || detailUvCt != 0L) {
                            collector.collect(new TrafficHomeDetailPageViewBean(
                                    "",
                                    "",
                                    "",
                                    homeUvCt,
                                    detailUvCt,
                                    jsonObject.getLong("ts")
                            ));
                        }
                    }
                }
        );
        //开10s窗扣,聚合指标,并且添加时间指标
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<Object> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean t1, TrafficHomeDetailPageViewBean t2) throws Exception {
                        t1.setHomeUvCt(t1.getHomeUvCt() + t2.getHomeUvCt());
                        t1.setGoodDetailUvCt(t1.getGoodDetailUvCt() + t2.getGoodDetailUvCt());
                        return t1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<Object> collector) throws Exception {
                        TrafficHomeDetailPageViewBean viewBean = iterable.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCurDate(curDate);
                        collector.collect(viewBean);
                    }
                }
        );

        //转为jsonStr格式,写入doris
    reduceDS.map(t->JSON.toJSONString(t))
            .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));

    }
}
