package com.gjn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gjn.base.UserLoginBean;
import com.gjn.util.DateFormatUtil;
import com.gjn.util.FlinkSinkUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *  @Package com.gjn.dws.DwsUserUserLoginWindow
 *  @Author jingnan.guo
 *  @Date 2025/4/17 11:47
     * @description: 独立用户以及回流用户聚合统计
 */
public class DwsUserUserLoginWindow {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        设置了检查点的超时时间为 60000 毫秒（即 60 秒）。如果在 60 秒内检查点操作没有完成，就会被视为失败。
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        当作业被取消时，检查点数据不会被删除，而是会保留下来，这样在后续需要时可以利用这些检查点数据进行恢复操作。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        两次检查点操作之间的最小间隔时间为 2000 毫秒（即 2 秒）。这是为了避免在短时间内频繁进行检查点操作，从而影响作业的正常处理性能。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        表示在 30 天内允许的最大失败次数为 3 次。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.seconds(3)));
//        状态后端用于管理 Flink 作业的状态数据，HashMapStateBackend 会将状态数据存储在 TaskManager 的内存中，适用于小规模的状态管理。
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("dwd_traffic_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String uid = jsonObj.getJSONObject("common").getString("uid");
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isNotEmpty(uid)
                                && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );

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

        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(o -> o.getJSONObject("common").getString("uid"));


        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastLoginDateState", String.class);
                        lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                        //获取上次登录日期
                        String lastLoginDate = lastLoginDateState.value();
                        //获取当前登录日期
                        Long ts = jsonObj.getLong("ts");
                        String curLoginDate = DateFormatUtil.tsToDate(ts);

                        Long uuCt = 0L;
                        Long backCt = 0L;
                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            //若状态中的末次登录日期不为 null，进一步判断。
                            if (!lastLoginDate.equals(curLoginDate)) {
                                //如果末次登录日期不等于当天日期则独立用户数 uuCt 记为 1，并将状态中的末次登录日期更新为当日，进一步判断。
                                uuCt = 1L;
                                lastLoginDateState.update(curLoginDate);
                                //如果当天日期与末次登录日期之差大于等于8天则回流用户数backCt置为1。
                                Long day = (ts - DateFormatUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                                if (day >= 8) {
                                    backCt = 1L;
                                }
                            }
                        } else {
                            //如果状态中的末次登录日期为 null，将 uuCt 置为 1，backCt 置为 0，并将状态中的末次登录日期更新为当日。
                            uuCt = 1L;
                            lastLoginDateState.update(curLoginDate);
                        }

                        if (uuCt != 0L || backCt != 0L) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                }
        );

        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
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
        //TODO 8.将聚合结果写到Doris
        reduceDS.print();
        reduceDS
                .map(new MapFunction<UserLoginBean, String>() {
                    @Override
                    public String map(UserLoginBean userLoginBean) throws Exception {
                        return JSON.toJSONString(userLoginBean);
                    }
                })
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));

        env.execute();
    }
}
