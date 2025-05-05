package com.xwz.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xwz.bean.UserLoginBean;
import com.xwz.function.BeanToJsonStrMapFunction;
import com.xwz.utils.DateFormatUtil;
import com.xwz.utils.FlinkSinkUtil;
import com.xwz.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.xwz.retail.v1.realtime.app.dws.DwsUserUserLoginWindow
 * @Author Wenzhen.Xie
 * @Date 2025/4/21 9:56
 * @description: DwsUserUserLoginWindow
 */

public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_traffic_page", "dws_user_user_login_window");

        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

//        jsonObjDS.print();

        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String uid = jsonObj.getJSONObject("common").getString("uid");
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isNotEmpty(uid)
                                && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );

//        filterDS.print();

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

        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));

        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastLoginDateState", String.class);
                        lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {

                        String lastLoginDate = lastLoginDateState.value();

                        Long ts = jsonObj.getLong("ts");
                        String curLoginDate = DateFormatUtil.tsToDate(ts);

                        long uuCt = 0L;
                        long backCt = 0L;
                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            if (!lastLoginDate.equals(curLoginDate)) {
                                uuCt = 1L;
                                lastLoginDateState.update(curLoginDate);
                                long day = (ts - DateFormatUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                                if (day >= 8) {
                                    backCt = 1L;
                                }
                            }
                        } else {
                            uuCt = 1L;
                            lastLoginDateState.update(curLoginDate);
                        }

                        if (uuCt != 0L) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                }
        );

//        beanDS.print();

        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) {
                        UserLoginBean bean = values.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        out.collect(bean);
                    }
                }
        );

        SingleOutputStreamOperator<String> jsonMap = reduceDS
                .map(new BeanToJsonStrMapFunction<>());

        jsonMap.print();

        jsonMap.sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));

        env.execute("DwsUserUserLoginWindow");
    }
}
