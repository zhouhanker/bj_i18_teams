package com.flink.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.flink.realtime.common.base.BaseApp;
import com.flink.realtime.common.constant.Constant;
import com.flink.realtime.common.util.DateFormatUtil;
import com.flink.realtime.common.util.FlinkSinkUtil;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

/**
 * @Package com.flink.realtime.dwd.log.DwdBaseLog
 * @Author guo.jia.hui
 * @Date 2025/4/11 8:41
 * @description: dwd层的日志处理
 */
public class DwdBaseLog extends BaseApp {
    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //如果转换的时候，没有发生异常，说明是标准的json，将数据传递下游
                    out.collect(jsonObj);
                } catch (Exception e) {
                    //如果转换的时候，发生了一场，说明不是标准的json，属于脏数据，将其开放到侧输出流中
                    ctx.output(dirtyTag, jsonStr);
                }
            }
        });
        jsonObjDS.print("标准的json");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);

        //对新老用户进行标记并修复
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }


            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {
                //获取is_new的值
                String is_new = jsonObj.getJSONObject("common").getString("is_new");
                //从状态中获取首次访问日期
                String lastVisitData = lastVisitDateState.value();
                //获取当前访问日期
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = DateFormatUtil.tsToDate(ts);
                //从状态中获取首次访问日期
                if ("1".equals(is_new)) {
                    //如果is_new的值为1
                    if (StringUtils.isEmpty(lastVisitData)) {
                        //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        lastVisitDateState.update(curVisitDate);
                    } else {
                        //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                        if (!lastVisitData.equals(curVisitDate)) {
                            is_new = "0";
                            jsonObj.getJSONObject("common").put("is_new", is_new);
                        }
                    }
                } else {
                    //如果 is_new 的值为 0
                    //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
                    // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                    if (StringUtils.isEmpty(lastVisitData)) {
                        String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                        lastVisitDateState.update(yesterDay);
                    }
                }
                return jsonObj;
            }
        });
        //fixedDS.print();
        //TODO 分流 错误日志、启动日志、曝光日志、动作日志，分别放在各自的侧输出流中，页面日志放在主流中
        HashMap<String, DataStream<String>> streamMap = splitStream(fixedDS);


        //将数据发送到不同的kafka主题中
        writerToKafka(streamMap);
    }

    private void writerToKafka(HashMap<String, DataStream<String>> streamMap) {
        streamMap.get(PAGE).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap.get(ERR).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap.get(START).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap.get(DISPLAY).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap.get(ACTION).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private HashMap<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        //分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) {
                JSONObject errJsonObj = jsonObj.getJSONObject("err");
                if (errJsonObj != null) {
                    //错误日志写到错误侧输出流中
                    ctx.output(errTag, jsonObj.toJSONString());
                    jsonObj.remove("err");
                }
                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                if (startJsonObj != null) {
                    //启动日志
                    ctx.output(startTag, jsonObj.toJSONString());
                } else {
                    //页面日志
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    //曝光日志
                    JSONArray displayArr = jsonObj.getJSONArray("displays");
                    if (displayArr != null && displayArr.size() > 0) {
                        //获取所有的曝光信息
                        for (int i = 0; i < displayArr.size(); i++) {
                            JSONObject displayJsonObj = displayArr.getJSONObject(i);
                            //将曝光日志写到曝光侧输出流
                            JSONObject newDisplayJsonObj = new JSONObject();
                            newDisplayJsonObj.put("common", commonJsonObj);
                            newDisplayJsonObj.put("page", pageJsonObj);
                            newDisplayJsonObj.put("displays", displayJsonObj);
                            ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                        }
                        jsonObj.remove("displays");
                    }
                    //动作日志
                    JSONArray actionArr = jsonObj.getJSONArray("actions");
                    if (actionArr != null && actionArr.size() > 0) {
                        for (int i = 0; i < actionArr.size(); i++) {
                            JSONObject actionJsonObj = actionArr.getJSONObject(i);
                            JSONObject newActionJsonObj = new JSONObject();
                            newActionJsonObj.put("common", commonJsonObj);
                            newActionJsonObj.put("page", pageJsonObj);
                            newActionJsonObj.put("actions", actionJsonObj);
                            ctx.output(actionTag, newActionJsonObj.toJSONString());
                        }
                        jsonObj.remove("actions");
                    }
                    //页面日志
                    out.collect(jsonObj.toJSONString());
                }
            }
        });
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        errDS.print("错误日志数据");
        startDS.print("启动日志数据");
        displayDS.print("曝光日志数据");
        actionDS.print("行动日志数据");
        pageDS.print("主流数据");
        //TOTO 分流 cu
        HashMap<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR, errDS);
        streamMap.put(START, startDS);
        streamMap.put(DISPLAY, displayDS);
        streamMap.put(ACTION, actionDS);
        streamMap.put(PAGE, pageDS);
        return streamMap;
    }
}
