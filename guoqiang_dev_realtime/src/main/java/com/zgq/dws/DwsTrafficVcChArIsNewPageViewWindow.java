package com.zgq.stream.realtime.v2.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zgq.bean.TrafficVcChArIsNewPageViewBean;
import com.zgq.function.BeanToJsonStrMapFunction;
import com.zgq.utils.DateFormatUtil;
import com.zgq.utils.FlinkSinkUtil;
import com.zgq.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
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
 * @Package com.zgq.stream.realtime.v2.app.dws.DwsTrafficVcChArIsNewPageViewWindow
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:54
 * @description:
 */

public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        KafkaSource <String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_traffic_page", "dws_traffic_vc_ch_ar_is_new_page_view_window");

        DataStreamSource <String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

//        kafkaStrDS.print();

        SingleOutputStreamOperator < JSONObject > jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        KeyedStream <JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> beanDS = midKeyedDS.map(
                new RichMapFunction <JSONObject, TrafficVcChArIsNewPageViewBean>() {
                    private ValueState <String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor <String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public TrafficVcChArIsNewPageViewBean map(JSONObject jsonObj) throws Exception {
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

                        return new TrafficVcChArIsNewPageViewBean(
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

        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficVcChArIsNewPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner <TrafficVcChArIsNewPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficVcChArIsNewPageViewBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );

//        withWatermarkDS.print();

        KeyedStream<TrafficVcChArIsNewPageViewBean, Tuple4 <String, String, String, String> > dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector <TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficVcChArIsNewPageViewBean bean) {
                        return Tuple4.of(bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew());
                    }
                }
        );

        WindowedStream <TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>, TimeWindow > windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction <TrafficVcChArIsNewPageViewBean>() {
                    @Override
                    public TrafficVcChArIsNewPageViewBean reduce(TrafficVcChArIsNewPageViewBean value1, TrafficVcChArIsNewPageViewBean value2) {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new WindowFunction <TrafficVcChArIsNewPageViewBean, TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficVcChArIsNewPageViewBean> input, Collector <TrafficVcChArIsNewPageViewBean> out) {
                        TrafficVcChArIsNewPageViewBean pageViewBean = input.iterator().next();
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

        SingleOutputStreamOperator<String> jsonMap = reduceDS
                .map(new BeanToJsonStrMapFunction <>());

        jsonMap.print();

        jsonMap.sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));

        env.execute("DwsTrafficVcChArIsNewPageViewWindow");
    }
}
