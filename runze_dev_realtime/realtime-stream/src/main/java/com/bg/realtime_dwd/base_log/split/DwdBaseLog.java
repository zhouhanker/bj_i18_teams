package com.bg.realtime_dwd.base_log.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.base.BaseApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.DateFormatUtil;
import com.bg.common.util.FlinkSinkUtil;
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
import java.util.Map;

/**
 * @Package com.bg.realtime_dwd.base_log.split.DwdBaseLog
 * @Author Chen.Run.ze
 * @Date 2025/4/9 19:48
 * @description: 日志分流
 */
public class DwdBaseLog extends BaseApp {
    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";


    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //TODO 对流中数据类型进行转换 做简单的ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDs = etl(kafkaSource);

        //TODO 对新老访客标记进行修复
        SingleOutputStreamOperator<JSONObject> fixDS = fixedNewAndOld(jsonObjDs);

        //TODO 分流 错误日志,启动日志,曝光日志,动作日志,页面日志
        Map<String, DataStream<String>> StreamMap = splitStream(fixDS);

        //TODO 将不同流数据写到不同Kafka主题中
        writeToKafka(StreamMap);


    }

    private void writeToKafka(Map<String, DataStream<String>> StreamMap) {
        StreamMap.get(PAGE).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        StreamMap.get(ERR).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        StreamMap.get(START).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        StreamMap.get(DISPLAY).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        StreamMap.get(ACTION).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixDS) {
        //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        SingleOutputStreamOperator<String> pageDS = fixDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) {
                        //错误日志
                        JSONObject err = jsonObject.getJSONObject("err");
                        if (err != null) {
                            context.output(errTag, jsonObject.toJSONString());
                            jsonObject.remove(err);
                        }

                        JSONObject start = jsonObject.getJSONObject("start");
                        if (start != null) {
                            //启动日志
                            context.output(startTag, jsonObject.toJSONString());
                        } else {
                            //页面日志
                            JSONObject common = jsonObject.getJSONObject("common");
                            JSONObject page = jsonObject.getJSONObject("page");
                            Long ts = jsonObject.getLong("ts");

                            //曝光日志
                            JSONArray displayArr = jsonObject.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject disJSONObject = displayArr.getJSONObject(i);
                                    //定义新的JSON对象
                                    JSONObject displayObj = new JSONObject();
                                    displayObj.put("common", common);
                                    displayObj.put("page", page);
                                    displayObj.put("display", disJSONObject);
                                    displayObj.put("ts", ts);
                                    context.output(displayTag, displayObj.toJSONString());
                                }
                                jsonObject.remove("displays");
                            }

                            //动作日志
                            JSONArray actionArr = jsonObject.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                //遍历出每个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject arrJSONObject = actionArr.getJSONObject(i);
                                    //定义一个新的JSON对象 用于封装动作信息
                                    JSONObject actionObj = new JSONObject();
                                    actionObj.put("common", common);
                                    actionObj.put("page", page);
                                    actionObj.put("action", arrJSONObject);
                                    //将动作日志写到动作侧输出流
                                    context.output(actionTag, actionObj.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }

                            //页面日志 写到主流中
                            collector.collect(jsonObject.toJSONString());

                        }


                    }
                }
        );

        SideOutputDataStream<String> errDs = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDs = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDs = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDs = pageDS.getSideOutput(actionTag);
        pageDS.print("页面-->");
        errDs.print("错误-->");
        startDs.print("启动-->");
        displayDs.print("曝光-->");
        actionDs.print("动作-->");

        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR,errDs);
        streamMap.put(START,startDs);
        streamMap.put(DISPLAY,displayDs);
        streamMap.put(ACTION,actionDs);
        streamMap.put(PAGE,pageDS);
        return streamMap;
    }

    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDs) {
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyDS = jsonObjDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> fixDS = keyDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取is_new的值
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                //从状态中获取首次访问日期
                String lastVisitDate = lastVisitDateState.value();
                //获取当前访问日期
                Long ts = jsonObject.getLong("ts");
                String curVisitDate = DateFormatUtil.tsToDate(ts);
                if ("1".equals(is_new)) {
                    //如果is_new的值为1
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        //	如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        lastVisitDateState.update(curVisitDate);
                    } else {
                        //	如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                        if (lastVisitDate.equals(curVisitDate)) {
                            is_new = "0";
                            jsonObject.getJSONObject("common").put("js_new", is_new);
                        }
                    }

                } else {
                    //如果 is_new 的值为 0
                    //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                        lastVisitDateState.update(yesterday);
                    }
                }


                return jsonObject;
            }
        });
        //使用Flink的状态变成完成修复
        fixDS.print("fix-->");
        return fixDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSource) {
        //定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        //ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                try {
                    JSONObject object = JSON.parseObject(s);
                    //如果没有异常,说明是标准json,将输出传到下游
                    collector.collect(object);
                } catch (Exception e) {
                    //如果没有异常,说明是标准json,将输出传到下游
                    context.output(dirtyTag, s);
                }
            }
        });
//        jsonObjDs.print("标准的json-->");
        SideOutputDataStream<String> dirtyDs = jsonObjDs.getSideOutput(dirtyTag);
//        dirtyDs.print("脏数据-->");

        //将侧输出流中的脏数据写到Kafka中
        KafkaSink<String> dirtyData = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDs.sinkTo(dirtyData);
        return jsonObjDs;
    }
}
