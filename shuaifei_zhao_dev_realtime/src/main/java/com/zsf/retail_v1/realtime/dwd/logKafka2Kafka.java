package com.zsf.retail_v1.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.zsf.retail_v1.realtime.constant.Constant;
import com.zsf.retail_v1.realtime.util.DateFormatUtil;
import com.zsf.retail_v1.realtime.util.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.zsf.retail_v1.realtime.dwd.logKafka2Kafka
 * @Author zhao.shuai.fei
 * @Date 2025/4/24 8:51
 * @description:
 */
public class logKafka2Kafka {
    private static final String START = "start";
    private static final String ERR = "err";
    private static final String DISPLAY = "display";
    private static final String ACTION = "action";
    private static final String PAGE = "page";

    @SneakyThrows
    public static void main(String[] args) {
        // 环境准备：流处理，
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);

        //kafka 读取数据
        DataStreamSource<String> logStr = KafkaUtil.getKafkaSource(env, "topic_log", "topic_066");

        //log：str --> obj
        SingleOutputStreamOperator<JSONObject> logObj = etl(logStr);

        //对新老访客标记进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = fixedNewAndOld(logObj);

        //错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流     页面日志-主流
        Map<String, DataStream<String>> streamMap = splitStream(fixedDS);

        //将不同流的数据写到kafka的不同主题中
        writeToKafka(streamMap);

        env.execute();
    }

    private static void writeToKafka(Map<String, DataStream<String>> streamMap) {
        DataStream<String> streamPAGE = streamMap.get(PAGE);
        streamPAGE.addSink(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));

        DataStream<String> streamERR = streamMap.get(ERR);
        streamERR.addSink(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

        DataStream<String> streamSTART = streamMap.get(START);
        streamSTART.addSink(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));

        DataStream<String> streamDISPLAY = streamMap.get(DISPLAY);
        streamDISPLAY.addSink(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));

        DataStream<String> streamACTION = streamMap.get(ACTION);
        streamACTION.addSink(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

    }
    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        //侧流：标签：存放数据
//        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //如果转换的时候，没有发生异常，说明是标准的json，将数据传递的下游
                    out.collect(jsonObj);
                } catch (Exception e) {
                    //如果转换的时候，发生了异常，说明不是是标准的json，属于脏数据，将其放到侧输出流中
//                    ctx.output(dirtyTag, jsonStr);

                }

            }
        });

        return jsonObjDS;
    }
    private static Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        //分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
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

        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR,errDS);
        streamMap.put(START,startDS);
        streamMap.put(DISPLAY,displayDS);
        streamMap.put(ACTION,actionDS);
        streamMap.put(PAGE,pageDS);
        return streamMap;
    }
    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //使用Flink的状态编程完成修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
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
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            //如果is_new的值为1
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                                lastVisitDateState.update(curVisitDate);
                            } else {
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
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }

                        return jsonObj;
                    }
                }
        );
        //fixedDS.print();
        return fixedDS;
    }
}
