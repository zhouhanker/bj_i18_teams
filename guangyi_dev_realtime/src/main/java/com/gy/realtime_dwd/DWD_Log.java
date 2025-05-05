package com.gy.realtime_dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gy.Base.BaseApp;
import com.gy.constat.constat;
import com.gy.utils.dataformtutil;
import com.gy.utils.finksink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;




/**
 * @Package realtime_Dwd.DWD_Log
 * @Author guangyi_zhou
 * @Date 2025/4/9 22:56
 * @description: 处理log
 */
//数据已经跑了
//15号
public class DWD_Log extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DWD_Log().start(10002, 4, "dwd_log", constat.TOPIC_LOG);
    }

    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        OutputTag<String> log_dirty_data = new OutputTag<String>("log_dirty_data"){};

        SingleOutputStreamOperator<JSONObject> log_filtration_v1 = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(log_dirty_data, s);
                }
            }
        });

        log_filtration_v1.getSideOutput(log_dirty_data).print("=========================>脏");
        KafkaSink<String> log_dirty_data_dwd = finksink.getkafkasink("log_dirty_data_dwd");
        //脏数据发送kafka
        log_filtration_v1.getSideOutput(log_dirty_data).sinkTo(log_dirty_data_dwd);
        //新老访客处理
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = log_filtration_v1.keyBy(o -> o.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> RepairDs = jsonObjectStringKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {
                //获取is_new的值
                String isNew = jsonObj.getJSONObject("common").getString("is_new");
                //从状态中获取首次访问日期
                String lastVisitDate = lastVisitDateState.value();
                //获取当前访问日期
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = dataformtutil.tsToDate(ts);

                if ("1".equals(isNew)) {
                    //如果is_new的值为1
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        lastVisitDateState.update(curVisitDate);
                    }
                    else {
                        //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                        if (!lastVisitDate.equals(curVisitDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    //如果 is_new 的值为 0
                    //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
                    // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        String yesterDay = dataformtutil.tsToDate(ts - 24 * 60 * 60 * 1000);
                        lastVisitDateState.update(yesterDay);

                    }
                }

                return jsonObj;
            }
        });
//        RepairDs.print("===============>修复后");
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        //分流
        SingleOutputStreamOperator<String> pageDS = RepairDs.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        //~~~错误日志~~~
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            //将错误日志写到错误侧输出流
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            //~~~启动日志~~~
                            //将启动日志写到启动侧输出流
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            //~~~页面日志~~~
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
                            //~~~曝光日志~~~
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                //遍历当前页面的所有曝光信息
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
                                    //定义一个新的JSON对象，用于封装遍历出来的曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", dispalyJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    //将曝光日志写到曝光侧输出流
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            //~~~动作日志~~~
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                //遍历出每一个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    //定义一个新的JSON对象，用于封装动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    //将动作日志写到动作侧输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

                            //页面日志  写到主流中
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面:");
        errDS.print("错误:");
        startDS.print("启动:");
        displayDS.print("曝光:");
        actionDS.print("动作:");
        errDS.sinkTo(finksink.getkafkasink(constat.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(finksink.getkafkasink(constat.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(finksink.getkafkasink(constat.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(finksink.getkafkasink(constat.TOPIC_DWD_TRAFFIC_ACTION));
        pageDS.sinkTo(finksink.getkafkasink(constat.TOPIC_DWD_TRAFFIC_PAGE));

    }
}

