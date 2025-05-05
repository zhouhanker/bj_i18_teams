package com.bg.realtime_dws.App;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.base.BaseApp;
import com.bg.common.bean.TrafficPageViewBean;
import com.bg.common.constant.Constant;
import com.bg.common.util.DateFormatUtil;
import com.bg.common.util.FlinkSinkUtil;
import com.bg.realtime_dws.function.BeanToJsonStrMapFunction;
import lombok.SneakyThrows;
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

/**
 * @Package com.bg.realtime_dws.App.DwsTrafficVcChArIsNewPageViewWindow
 * @Author Chen.Run.ze
 * @Date 2025/4/15 20:44
 * @description: 按照版本、地区、渠道、新老访客对pv、uv、sv、dur进行聚合统计
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    @SneakyThrows
    public static void main(String[] args){
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
//        jsonObjDS.print();
        //TODO 2.按照mid对流中数据进行分组（计算UV）
        KeyedStream<JSONObject, String> midKeyedDs = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 3.再次对流中数据进行类型转奂jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDs.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
            private  ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());//保留一天
                lastVisitDateState =getRuntimeContext().getState(valueStateDescriptor);
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
                long uvCt = 0L;
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
//        1> TrafficPageViewBean(stt=, edt=, cur_date=, vc=v2.1.134, ch=oppo, ar=21, isNew=1, uvCt=0, svCt=0, pvCt=1, durSum=8053, ts=1744007276534)
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                //WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<TrafficPageViewBean>) (bean, recordTimestamp) -> bean.getTs()
                        )
        );

        //TODO 5.分组--按照统计的维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean){
                        return Tuple4.of(bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew());
                    }
                }
        );
        //TODO 6.开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 7. 聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2){
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out){
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
        //TODO 8.将聚合的结果写到Doris表
        reduceDS
                .map(new BeanToJsonStrMapFunction<>())  //在向Doris写数据前，将流中统计的实体类对象转换为json格式字符串
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));

    }
}
