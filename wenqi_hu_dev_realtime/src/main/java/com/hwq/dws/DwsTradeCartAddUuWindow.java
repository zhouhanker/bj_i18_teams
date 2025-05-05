package com.hwq.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.common.bean.UserLoginBean;
import com.hwq.common.until.DateFormatUtil;
import com.hwq.common.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * @Package 独立用户以及回流用户聚合统计
 * @Author hu.wen.qi
 * @Date 2025/5/4
 * @description: 1
 */
public class DwsTradeCartAddUuWindow {
    @SuppressWarnings("Convert2Lambda")
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
        //page.print();

        SingleOutputStreamOperator<String> page_filter = page.filter((FilterFunction<String>) s -> {
            boolean b = JSON.isValid(s);
            if (!b) {
                return false;
            }
            JSONObject all = JSON.parseObject(s);
            String s1 = all.getJSONObject("common").getString("uid");
            String s2 = all.getJSONObject("page").getString("last_page_id");
            return s1 != null && s2 != null;
        });

        //page_filter.print();

        //水位线
        SingleOutputStreamOperator<String> page_water = page_filter.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<String>) (s, l) -> {
            JSONObject jsonObject = JSON.parseObject(s);
            return jsonObject.getLong("ts");
        }));

        page_water.print();

        SingleOutputStreamOperator<UserLoginBean> page_fen = page_water.keyBy(o -> JSON.parseObject(o).getJSONObject("common").getString("uid"))
                .process(new KeyedProcessFunction<String, String, UserLoginBean>() {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> value1 = new ValueStateDescriptor<>("state", String.class);
                        state = getRuntimeContext().getState(value1);
                    }

                    @Override
                    public void processElement(String s, KeyedProcessFunction<String, String, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                        //获取上次登录日期
                        String lastDate = state.value();
                        //获取当前登陆日期
                        JSONObject jsonObject = JSON.parseObject(s);
                        Long ts = jsonObject.getLong("ts");
                        String TsDate = DateFormatUtil.tsToDate(ts);
                        //记录数据变化
                        long uuCt = 0L;
                        long backCt = 0L;
                        if (StringUtils.isNotEmpty(lastDate)) {
                            //若状态中的末次登录日期不为 null，进一步判断。
                            if (!lastDate.equals(TsDate)) {
                                //如果末次登录日期不等于当天日期则独立用户数 uuCt 记为 1，并将状态中的末次登录日期更新为当日，进一步判断。
                                uuCt = 1L;
                                state.update(TsDate);
                                //如果当天日期与末次登录日期之差大于等于8天则回流用户数backCt置为1。
                                long day = (ts - DateFormatUtil.dateToTs(lastDate)) / 1000 / 60 / 60 / 24;
                                if (day >= 8) {
                                    backCt = 1L;
                                }
                            }
                        } else {
                            //如果状态中的末次登录日期为 null，将 uuCt 置为 1，backCt 置为 0，并将状态中的末次登录日期更新为当日。
                            uuCt = 1L;
                            state.update(TsDate);
                        }
                        if (uuCt != 0L) {
                            collector.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                });

        //page_fen.print();

        //TODO 6.开窗
        AllWindowedStream<UserLoginBean, TimeWindow> page_window = page_fen.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 7.聚合
        SingleOutputStreamOperator<UserLoginBean> reduceDS = page_window.reduce(
                (ReduceFunction<UserLoginBean>) (value1, value2) -> {
                    value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                    value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                    return value1;
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) {
                        //获取数据
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
        //reduceDS.print();

        SingleOutputStreamOperator<String> map = reduceDS.map(JSON::toJSONString);
        map.print();
        //map.sinkTo(SinkDoris.getDorisSink("dws_to_doris","user_login_bean"));



        env.execute();
    }
}
