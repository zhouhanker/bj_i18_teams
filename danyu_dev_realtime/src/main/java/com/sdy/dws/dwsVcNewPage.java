package com.sdy.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;

import com.sdy.bean.DateFormatUtil;
import com.sdy.bean.KafkaUtil;
import com.sdy.dws.util.PageViewTable;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
 * @Package com.sdy.retail.v1.realtime.dws.dwsVcNewPage
 * @Author danyu-shi
 * @Date 2025/4/15 21:43
 * @description:
 */
public class dwsVcNewPage {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        // 1. 解析数据, 封装 pojo 中

        DataStreamSource<String> kafkasource = KafkaUtil.getKafkaSource(env, "stream_dwdpage_danyushi", "dws_Page");
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkasource.map(JSON::parseObject);
        // 2.按照mid对流中数据进行分组（计算UV）
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<PageViewTable> beanDS = midKeyedDS.map(new RichMapFunction<JSONObject, PageViewTable>() {
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters)  {
                ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @SneakyThrows
            @Override
            public PageViewTable map(JSONObject jsonObj)  {
                JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                //从状态中获取当前设置上次访问日期
                String lastVisitDate = lastVisitDateState.value();
                //获取当前访问日期
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = DateFormatUtil.tsToDate(ts);
                Long uvCt = 0L;
                if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                    uvCt = 1L;
                    lastVisitDateState.update(curVisitDate);
                }

                String lastPageId = pageJsonObj.getString("last_page_id");

                Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

                return new PageViewTable(
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

        // 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<PageViewTable> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                //WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                WatermarkStrategy
                        .<PageViewTable>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PageViewTable>() {
                                    @Override
                                    public long extractTimestamp(PageViewTable bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );

        // 5.分组--按照统计的维度进行分组
        KeyedStream<PageViewTable, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<PageViewTable, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(PageViewTable bean)  {
                        return Tuple4.of(bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew());
                    }
                }
        );


        WindowedStream<PageViewTable, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));



        // 7.聚合计算
        SingleOutputStreamOperator<PageViewTable> reduceDS = windowDS.reduce(
                new ReduceFunction<PageViewTable>() {
                    @Override
                    public PageViewTable reduce(PageViewTable value1, PageViewTable value2)  {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new WindowFunction<PageViewTable, PageViewTable, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<PageViewTable> input, Collector<PageViewTable> out)  {
                        PageViewTable pageViewBean = input.iterator().next();
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

        // 8.将聚合的结果写到Doris表
//        reduceDS
//                //在向Doris写数据前，将流中统计的实体类对象转换为json格式字符串
//                .map(new MapFunction<PageViewTable, String>() {
//                    @Override
//                    public String map(PageViewTable bean)  {
//                        SerializeConfig config = new SerializeConfig();
//                        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
//                        return JSON.toJSONString(bean, config);
//                    }
//                })
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));



        env.execute();

    }
}
