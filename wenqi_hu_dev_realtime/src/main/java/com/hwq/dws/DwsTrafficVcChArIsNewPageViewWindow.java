package com.hwq.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.common.bean.TrafficPageViewBean;
import com.hwq.common.until.DateFormatUtil;
import com.hwq.common.until.KafkaUtil;
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
 * @Package com.hwq.dws.DwsTrafficVcChArIsNewPageViewWindow
 * @Author hu.wen.qi
 * @Date 2025/5/4
 * @description: 1
 * 按照版本、地区、渠道、新老访客对pv、uv、sv、dur进行聚合统计
 *   需要启动的进程
 *     zk、kafka、flume、doris、DwdBaseLog、DwsTrafficVcChArIsNewPageViewWindow
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
// 精确一次语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 检查点超时时间（2分钟）
        checkpointConfig.setCheckpointTimeout(120000);
// 最小间隔：500毫秒（防止检查点过于频繁）
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
// 最大并发检查点数
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        DataStreamSource<String> page = KafkaUtil.getKafkaSource(env, "page_log", "a1");
        page.print();

        SingleOutputStreamOperator<JSONObject> jsonObject = page.map(JSON::parseObject);

        //jsonObject.print();

        KeyedStream<JSONObject, String> keyedStream = jsonObject.keyBy(o -> o.getJSONObject("common").getString("mid"));
//        keyedStream.print();

        SingleOutputStreamOperator<TrafficPageViewBean> beanDs = keyedStream.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
            ValueState<String> state;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<String> value1 = new ValueStateDescriptor<>("state", String.class);
                //设置状态时间
                value1.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                state = getRuntimeContext().getState(value1);
            }

            @Override
            public TrafficPageViewBean map(JSONObject jsonObject) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page1 = jsonObject.getJSONObject("page");
                //从状态中获取当前设置上次访问日期
                String lastDate = state.value();
                //设置当前访问日期
                Long ts = jsonObject.getLong("ts");
                String tsDate = DateFormatUtil.tsToDate(ts);
                long uvCt = 0L;
                //如果上一次访问日期不为空并且上一次日期不包含今天访问日期
                if (StringUtils.isEmpty(lastDate) || !lastDate.equals(tsDate)) {
                    uvCt = 1L;
                    state.update(tsDate);
                }
                String lastPageId = page1.getString("last_page_id");

                Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

                return new TrafficPageViewBean(
                        "",
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        uvCt,
                        svCt,
                        1L,
                        common.getLong("during_time"),
                        ts
                );
            }
        });

        //beanDs.print();

        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> bean_water = beanDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                return trafficPageViewBean.getTs();
            }
        }));

        //bean_water.print();

        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> bean_Ds = bean_water.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) {
                return Tuple4.of(trafficPageViewBean.getVc(),
                        trafficPageViewBean.getCh(),
                        trafficPageViewBean.getAr(),
                        trafficPageViewBean.getIsNew()
                );
            }
        });

        //bean_Ds.print();

        //TODO 6.开窗
        //以滚动事件时间窗口为例，分析如下几个窗口相关的问题
        //窗口对象时候创建:当属于这个窗口的第一个元素到来的时候创建窗口对象
        //窗口的起始结束时间（窗口为什么是左闭右开的）
        //向下取整：long start =TimeWindow.getWindowStartWithOffset(timestamp, (globalOffset + staggerOffset) % size, size);
        //窗口什么时候触发计算  window.maxTimestamp() <= ctx.getCurrentWatermark()
        //窗口什么时候关闭     watermark >= window.maxTimestamp() + allowedLateness

        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS =
                bean_Ds.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));




        //TODO 7.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        if (value1.getDurSum()!=null || value2.getDurSum()!= null){
                            //noinspection DataFlowIssue
                            value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        }
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
       //reduceDS.print();


        SingleOutputStreamOperator<String> map = reduceDS.map(JSON::toJSONString);
        map.print();
        //map.sinkTo(SinkDoris.getDorisSink("dws_to_doris","dws_traffic_vcchar_isnew_pageviewwindow1"));



        env.execute();
    }
}
