package com.dws.realtime_dws_user_user_login_window;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.Bean.UserLoginBean;
import com.common.base.BaseApp;
import com.common.constant.Constant;
import com.common.utils.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
 * @author Felix
 * @date 2024/6/11
 * 独立用户以及回流用户聚合统计
 */
public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsUserUserLoginWindow().start(
                10024,
                4,
                "dws_user_user_login_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE,
                "source28"
        );

    }
    @Override
    public void Handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
//   对流中数据进行转化
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSource.map(JSON::parseObject);
//        过滤首页及详情页
        SingleOutputStreamOperator<JSONObject> filterDs = jsonObjDs.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String uid = jsonObj.getJSONObject("common").getString("uid");
                String lastpageId = jsonObj.getJSONObject("page").getString("last_page_id");
                return StringUtils.isNotEmpty(uid)&&("login".equals(lastpageId)||StringUtils.isEmpty(lastpageId));
            }
        });
//    filterDs.print();

        // 4.按照uid进行分组
        KeyedStream<JSONObject, String> keyedDS = filterDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));
//        keyedDS.print();
        //      使用Flink的状态编程  判断是否为独立用户以及回流用户
        SingleOutputStreamOperator<UserLoginBean> beanDs = keyedDS.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
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
                    out.collect(new UserLoginBean("",  "","", backCt, uuCt, ts));
                }
            }
        }
        );
//        beanDs.print();
        //.指定watermark
        SingleOutputStreamOperator<UserLoginBean> withWatermarkDS = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<UserLoginBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<UserLoginBean>() {
                                    @Override
                                    public long extractTimestamp(UserLoginBean userLoginBean, long recordTimestamp) {
                                        return userLoginBean.getTs();
                                    }
                                }
                        )
        );
//      开窗
        AllWindowedStream<UserLoginBean, TimeWindow> windowDs = withWatermarkDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
//        聚合
        SingleOutputStreamOperator<UserLoginBean> reduceDs = windowDs.reduce(new ReduceFunction<UserLoginBean>() {
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
//                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        bean.setStt(stt);
//                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        out.collect(bean);
                    }
                }
        );
        reduceDs.print();
//        reduceDs
//                .map(new BeanToJsonStrMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));
    }
}
