package com.realtime_dws;

import com.Base.BaseApp;
import com.Constat.constat;
import com.bean.TrafficPageViewBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.utils.dataformtutil;
import com.utils.finksink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package realtime_dws.dws_traffic_vc_ch_ar_s_new_page_view_window
 * @Author ayang
 * @Date 2025/4/15 9:07
 * @description: 12.2流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 */
//15号分区和数据

public class dws_traffic_vc_ch_ar_s_new_page_view_window extends BaseApp {
    public static void main(String[] args) throws Exception {
            new dws_traffic_vc_ch_ar_s_new_page_view_window().start(10010,4,"TOPIC_DWD_TRAFFIC_PAGE", constat.TOPIC_DWD_TRAFFIC_PAGE);

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> data_v1 = kafkaStrDS.map(JSON::parseObject);
        KeyedStream<JSONObject, String> keyedStream = data_v1.keyBy(o -> o.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = keyedStream.map(
                new RichMapFunction<JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> lastVisitDateState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastVisitDateState1 = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        lastVisitDateState1.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                            lastVisitDateState=getRuntimeContext().getState(lastVisitDateState1);

                    }

                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                        //从状态中获取当前设置上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = dataformtutil.tsToDate(ts);
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
//        beanDS.print();
//        4> TrafficPageViewBean(stt=, edt=, cur_date=, vc=v2.1.134, ch=360, ar=10, isNew=1, uvCt=0, svCt=0, pvCt=1, durSum=18231, ts=1744031377164)
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewBeanSingleOutputStreamOperator = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                                return trafficPageViewBean.getTs();

                            }
                        })

        );
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = trafficPageViewBeanSingleOutputStreamOperator.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple4.of(bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew());
                    }
                }
        );
//        dimKeyedDS.print();
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.
                of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

//        3> TrafficPageViewBean(stt=, edt=, cur_date=, vc=v2.1.111, ch=xiaomi, ar=13, isNew=1, uvCt=0, svCt=0, pvCt=1, durSum=13771, ts=1744038466285)
        SingleOutputStreamOperator<TrafficPageViewBean> result = windowDS
                // 定义一个时间窗口，这里假设是 5 分钟的滚动窗口，你可以根据实际需求修改
                .reduce(
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
                            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                                TrafficPageViewBean pageViewBean = iterable.iterator().next();
                                String s = dataformtutil.tsToDateTime(timeWindow.getStart());
                                String s1 = dataformtutil.tsToDateTime(timeWindow.getEnd());
                                String s2 = dataformtutil.tsToDate(timeWindow.getStart());
                                pageViewBean.setStt(s);
                                pageViewBean.setEdt(s1);
                                pageViewBean.setCur_date(s2);
                                collector.collect(pageViewBean);
                            }
                        }
                );
//        result.print();
//        2> TrafficPageViewBean(stt=2025-04-16 23:31:10, edt=2025-04-16 23:31:20, cur_date=2025-04-16, vc=v2.1.134, ch=oppo, ar=11, isNew=1, uvCt=0, svCt=0, pvCt=1, durSum=19526, ts=1744817474280)

        SingleOutputStreamOperator<String> map = result
                .map(new MapFunction<TrafficPageViewBean, String>() {

                    @Override
                    public String map(TrafficPageViewBean trafficPageViewBean) throws Exception {
                     return JSON.toJSONString(trafficPageViewBean);
                    }
                });

        map.sinkTo(finksink.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
//   map.print();

    }

}
