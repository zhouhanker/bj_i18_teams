package com.flink.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.flink.realtime.common.base.BaseApp;
import com.flink.realtime.common.bean.TrafficPageViewBean;
import com.flink.realtime.common.constant.Constant;
import com.flink.realtime.common.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @Package flink.realtime.dws.app.DwsTrafficVcChArisNewPageViewWindow
 * @Author guo.jia.hui
 * @Date 2025/4/17 20:49
 * @description: 按照版本、地区、渠道、新老访客对PV、UV、SV、dur进行聚合统计
 */
public class DwsTrafficVcChArisNewPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArisNewPageViewWindow().start(
                10025,
                1,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //kafkaStrDS.print();
        //TODO 1.对流中数据进行类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        KeyedStream<JSONObject, String> mid = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 2.对流中数据进行类型转换 jsonObj -> 统计的实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = mid.map(
                new RichMapFunction<JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

                    }

                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject common = jsonObj.getJSONObject("common");

                        //从状态中获取当前位置上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        Long uvCt = 0L;
                        if (StringUtils.isEmpty(lastVisitDate) || lastVisitDate.equals(curVisitDate)) {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }

                        return new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                uvCt,
                                1L,
                                jsonObj.getJSONObject("page").getLong("during_time"),
                                ts
                        );
                    }
                }
        );
        //beanDS.print();
        //TODO 3.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );
        //TODO 4.分组 --按照统计的维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) {
                        return Tuple4.of(
                                bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew()
                        );
                    }
                }
        );
        //TODO 5.开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS =
                dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 6.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) {
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
        reduceDS.print();
        //TODO 7.将聚合的结果写到Doris表
//        reduceDS
//                .map(new BeanToJsonStrMapFunction<TrafficPageViewBean>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }
}
