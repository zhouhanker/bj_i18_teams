package com.ytx.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.stream.common.utils.DateTimeUtils;
import com.ytx.base.BaseApp;
import com.ytx.constant.Constant;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class DwdBaselog extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaselog().start(10011,4,"my_group", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDs= kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    out.collect(jsonObj);
                } catch (Exception e) {
                    ctx.output(dirtyTag, jsonStr);
                }
            }
        });
//     jsonObjDs.print("标准json");
        SideOutputDataStream<String> dirtyDs = jsonObjDs.getSideOutput(dirtyTag);
//        dirtyDs.print("脏数据");
//        对新老访客标记进行修复
        KeyedStream<JSONObject, String> keyedDs = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> fixeDs = keyedDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastVisitState", String.class);
                lastVisitState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
//                从状态中获取首次询问日期
                String lastVisitDate = lastVisitState.value();
//                获取当前访问日期
                Long ts = jsonObject.getLong("ts");
                String curVisitDate = DateTimeUtils.tsToDate(ts);
                if ("1".equals(isNew)) {
                    // 如果is_new的值为1
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        lastVisitState.update(curVisitDate);
                    } else {
                        //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                        if (!lastVisitDate.equals(curVisitDate)) {
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("isNew", isNew);
                        }
                    }
                    //如果键控状态不为 null，且首次访问日期是当日，说明访问的是新访客，不做操作；

                } else {
                    //② 如果 is_new 的值为 0
                    //如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                    //如果键控状态不为 null，说明程序已经维护了首次访问日期，不做操作。
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        String yesterDay = DateTimeUtils.tsToDate(ts - 24 * 60 * 1000);
                        lastVisitState.update(yesterDay);
                    }
                }
                return jsonObject;
            }
        });
//       fixeDs.print();
//      分流
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displaysTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        SingleOutputStreamOperator<String> pageDs = fixeDs.process(
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

        SideOutputDataStream<String> errDs = pageDs.getSideOutput(errTag);
        SideOutputDataStream<String> startDs = pageDs.getSideOutput(startTag);
        SideOutputDataStream<String> displayDs = pageDs.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDs = pageDs.getSideOutput(actionTag);
        pageDs.print("页面");
        errDs.print("错误");
        startDs.print("启动");
        displayDs.print("曝光");
        actionDs.print("动作");
//        将不同的数据写到kafka主题中
//        pageDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
//        errDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
//        startDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
//        displayDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
//        actionDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));



    }
}
