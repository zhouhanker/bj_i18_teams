package com.sdy.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sdy.bean.DateFormatUtil;
import com.sdy.bean.KafkaUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * @Package com.sdy.retail.v1.realtime.dwd.csdwd_util
 * @Author danyu-shi
 * @Date 2025/4/10 20:38
 * @description:
 */
public class DwdBaseLog {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.enableCheckpointing(60000);

        DataStreamSource<String> csdwdUtil = KafkaUtil.getKafkaSource(env,"stream-dev1-danyushi", "Dwd_Base_Log");

//        csdwdUtil.print();

        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        SingleOutputStreamOperator<JSONObject> jsonObjDS = csdwdUtil.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonstr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonstr);
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            context.output(dirtyTag, jsonstr);
                        }


                    }
                }
        );
//        jsonObjDS.print("标准数据--->");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//        dirtyDS.print("脏数据---->");

        KafkaSink<String> sink = KafkaUtil.getKafkaSink("stream-dwdlog-danyushi");
//        dirtyDS.sinkTo(sink);


        //新老客户

        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {

                        String isnew = jsonObj.getJSONObject("common").getString("is_new");
                        //获取首次日期
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");

                        String tsToDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isnew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //获取此时日期
                                lastVisitDateState.update(tsToDate);
                            } else {

                                if (!lastVisitDate.equals(tsToDate)) {
                                    isnew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isnew);
                                }
                            }

                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }

                        }
                        return jsonObj;
                    }
                }
        );
        fixedDS.print();
        OutputTag<String> errTag = new OutputTag<String>("errTag"){};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>( "actionTag") {};

        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            //错误日志
                            context.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");

                        if (startJsonObj != null) {
                            //启动日志
                            context.output(startTag, jsonObj.toJSONString());
                        } else {
                            //页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            //曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJSONObj = displayArr.getJSONObject(i);

                                    JSONObject newDispplayjsonObj = new JSONObject();
                                    newDispplayjsonObj.put("common", commonJsonObj);
                                    newDispplayjsonObj.put("page", pageJsonObj);
                                    newDispplayjsonObj.put("display", displayJSONObj);
                                    newDispplayjsonObj.put("ts", ts);
                                    context.output(displayTag, newDispplayjsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }
                            //动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject arrJSONObj = actionArr.getJSONObject(i);

                                    JSONObject newArrJsonObj = new JSONObject();
                                    newArrJsonObj.put("common", commonJsonObj);
                                    newArrJsonObj.put("page", pageJsonObj);
                                    newArrJsonObj.put("action", arrJSONObj);
                                    context.output(actionTag, newArrJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

                            //页面
                            collector.collect(jsonObj.toJSONString());

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


        //传入kafka
//        pageDS.sinkTo(KafkaUtil.getKafkaSink("stream_dwdpage_danyushi"));
//        errDS.sinkTo(KafkaUtil.getKafkaSink("stream_dwderr_danyushi"));
//        startDS.sinkTo(KafkaUtil.getKafkaSink("stream_dwdstart_danyushi"));
//        displayDS.sinkTo(KafkaUtil.getKafkaSink("stream_dwddisplay_danyushi"));
//        actionDS.sinkTo(KafkaUtil.getKafkaSink("stream_dwdaction_danyushi"));


        env.execute();
    }

}

