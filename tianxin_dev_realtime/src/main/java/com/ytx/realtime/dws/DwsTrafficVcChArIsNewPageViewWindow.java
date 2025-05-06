package com.ytx.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.stream.common.utils.DateTimeUtils;
import com.ytx.base.BaseApp;
import com.ytx.bean.TrafficPageViewBean;
import com.ytx.constant.Constant;
import com.ytx.util.DateFormatUtil;
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

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10025,4, "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
//      对流中数据进行类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSource.map(JSON::parseObject);
//        按照mid对流中数据进行分组（计算UV）
        KeyedStream<JSONObject, String> midKeyDs = jsonObjDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
//        再次对流中数据进行类型转换  jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDs = midKeyDs.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
            private ValueState<String> lastVisitDataState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor<>("lastVisitDataState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastVisitDataState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                JSONObject pageJsonObj = jsonObj.getJSONObject("page");
//                从状态中获取当前设置上次访问日期
                String lastVisitDate = lastVisitDataState.value();
//                获取当前访问日期
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = DateTimeUtils.tsToDate(ts);
                Long uvCt = 0L;
                if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                    uvCt = 1L;
                    lastVisitDataState.update(curVisitDate);
                }
                String lastPageId = pageJsonObj.getString("last_page_id");
                Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;
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
        });
//        beanDs.print();
//        指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS  = beanDs.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TrafficPageViewBean>forMonotonousTimestamps().
                withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean bean, long l) {
                        return bean.getTs();
                    }
                }));
//        分组按照统计的维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyDs = withWatermarkDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                return Tuple4.of(bean.getVc(), bean.getCh(), bean.getAr(), bean.getIs_new());
            }
        });
//      开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDs =
                dimKeyDs.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
//
//        聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDs = windowDs.reduce(new ReduceFunction<TrafficPageViewBean>() {
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
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        pageViewBean.setStt(stt);
                        pageViewBean.setCur_date(curDate);
                        out.collect(pageViewBean);
                    }
                });
          reduceDs.print();
//        8.将聚合的结果写到Doris表

        //在向Doris写数据前，将流中统计的实体类对象转换为json格式字符串
//     reduceDs.map(new BeanToJsonStrMapFunction<TrafficPageViewBean>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));

    }
}
