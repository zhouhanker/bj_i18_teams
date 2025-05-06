package com.zzw.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zzw.constant.Constant;
import com.zzw.utils.DateFormatUtil;
import com.zzw.utils.FlinkSinkUtil;
import com.zzw.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.lzy.app.bwd.DwdBaseLog
 * @Author zheyuan.liu
 * @Date 2025/4/11 10:35
 * @description: DwdBaseLog
 */

public class DwdBaseLog {
//    // 定义一些常量，用于标识不同类型的日志
//    private static final String START = "start";
//    private static final String ERR = "err";
//    private static final String DISPLAY = "display";
//    private static final String ACTION = "action";
//    private static final String PAGE = "page";
//
//    public static void main(String[] args) throws Exception {
//        // 获取Flink的流式执行环境，这是执行Flink流处理作业的基础
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 设置作业的并行度为4，即作业在运行时算子会以4个并行实例执行
//        env.setParallelism(4);
//
//        // 启用检查点机制，每隔5000毫秒（5秒）生成一个检查点
//        // 采用EXACTLY_ONCE模式，确保数据处理的准确性，保证每条数据仅被处理一次
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//
//        // 使用自定义工具类FlinkSourceUtil获取Kafka数据源
//        // 从指定的Kafka主题Constant.TOPIC_LOG中消费数据，消费者组为"dwd_log"
//        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_LOG, "dwd_log");
//
//        // 基于Kafka数据源创建数据流，不使用水印策略，将数据源命名为"Kafka_Source"
//        DataStreamSource<String> kafkaStrDS = env
//                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
//
//        // 注释掉的代码，若取消注释，会将从Kafka读取的原始数据打印到控制台，用于调试
////        kafkaStrDS.print();
//
//        // 定义一个侧输出流标签，用于标识脏数据（解析失败的数据）
//        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
//
//        // 对Kafka读取的字符串数据流进行处理，尝试将每个字符串解析为JSONObject
//        // 解析成功的数据正常收集输出，解析失败的数据通过侧输出流输出
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
//                new ProcessFunction<String, JSONObject>() {
//                    @Override
//                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                        try {
//                            // 尝试将字符串解析为JSONObject对象
//                            JSONObject jsonObj = JSON.parseObject(jsonStr);
//                            // 解析成功，将对象收集到主输出流
//                            out.collect(jsonObj);
//                        } catch (Exception e) {
//                            // 解析失败，将原始字符串通过侧输出流输出
//                            ctx.output(dirtyTag, jsonStr);
//                        }
//                    }
//                }
//        );
//
//        // 注释掉的代码，若取消注释，会将解析后的JSONObject数据流打印到控制台，用于调试
////        jsonObjDS.print();
//
//        // 获取解析过程中产生的脏数据侧输出流
//        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//
//        // 将脏数据通过自定义工具类FlinkSinkUtil写入到名为"dirty_data"的Kafka主题中
//        dirtyDS.sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));
//
//        // 根据JSONObject中"common"字段下的"mid"值对数据流进行分组，得到KeyedStream
//        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
//
//        // 对分组后的数据流进行映射处理，主要处理"is_new"标识相关逻辑
//        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
//                new RichMapFunction<JSONObject, JSONObject>() {
//                    // 定义一个ValueState用于存储上次访问日期
//                    private ValueState<String> lastVisitDateState;
//
//                    @Override
//                    public void open(Configuration parameters) {
//                        // 定义ValueState的描述符
//                        ValueStateDescriptor<String> valueStateDescriptor =
//                                new ValueStateDescriptor<>("lastVisitDateState", String.class);
//                        // 为ValueState设置生存时间（TTL），10秒
//                        // 配置更新策略为在创建和写入时更新
//                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
//                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                                .build());
//                        // 从运行时上下文获取ValueState实例
//                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
//                    }
//
//                    @Override
//                    public JSONObject map(JSONObject jsonObj) throws Exception {
//                        // 获取"is_new"标识的值
//                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
//                        // 获取上次访问日期
//                        String lastVisitDate = lastVisitDateState.value();
//                        // 获取当前时间戳
//                        Long ts = jsonObj.getLong("ts");
//                        // 将时间戳转换为日期字符串
//                        String curVisitDate = DateFormatUtil.tsToDate(ts);
//
//                        if ("1".equals(isNew)) {
//                            // 如果标识为新用户，且上次访问日期为空
//                            if (StringUtils.isEmpty(lastVisitDate)) {
//                                // 更新上次访问日期为当前日期
//                                lastVisitDateState.update(curVisitDate);
//                            } else {
//                                // 如果上次访问日期与当前日期不同
//                                if (!lastVisitDate.equals(curVisitDate)) {
//                                    // 将"is_new"标识设为"0"，表示不再是新用户
//                                    isNew = "0";
//                                    jsonObj.getJSONObject("common").put("is_new", isNew);
//                                }
//                            }
//                        } else {
//                            // 如果不是新用户，且上次访问日期为空
//                            if (StringUtils.isEmpty(lastVisitDate)) {
//                                // 计算昨天的日期并更新为上次访问日期
//                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
//                                lastVisitDateState.update(yesterDay);
//                            }
//                        }
//
//                        return jsonObj;
//                    }
//                }
//        );
//        // 注释掉的代码，若取消注释，会将处理后的JSONObject数据流打印到控制台，用于调试
////        fixedDS.print();
//
//        // 定义多个侧输出流标签，分别用于标识错误日志、启动日志、曝光日志、动作日志
//        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
//        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
//        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
//        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
//        // 对处理后的JSONObject数据流进行分流操作
//        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
//                new ProcessFunction<JSONObject, String>() {
//                    @Override
//                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
//                        // 处理错误日志
//                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
//                        if (errJsonObj != null) {
//                            // 如果存在错误日志，将该日志通过侧输出流输出
//                            ctx.output(errTag, jsonObj.toJSONString());
//                            // 从原始JSON对象中移除错误日志字段
//                            jsonObj.remove("err");
//                        }
//
//                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
//                        if (startJsonObj != null) {
//                            // 如果存在启动日志，将该日志通过侧输出流输出
//                            ctx.output(startTag, jsonObj.toJSONString());
//                        } else {
//                            // 处理页面日志
//                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
//                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
//                            Long ts = jsonObj.getLong("ts");
//                            // 处理曝光日志
//                            JSONArray displayArr = jsonObj.getJSONArray("displays");
//                            if (displayArr != null && displayArr.size() > 0) {
//                                for (int i = 0; i < displayArr.size(); i++) {
//                                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
//                                    // 创建新的JSONObject，包含公共信息、页面信息和曝光信息
//                                    JSONObject newDisplayJsonObj = new JSONObject();
//                                    newDisplayJsonObj.put("common", commonJsonObj);
//                                    newDisplayJsonObj.put("page", pageJsonObj);
//                                    newDisplayJsonObj.put("display", dispalyJsonObj);
//                                    newDisplayJsonObj.put("ts", ts);
//                                    // 将曝光日志通过侧输出流输出
//                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
//                                }
//                                // 从原始JSON对象中移除曝光日志字段
//                                jsonObj.remove("displays");
//                            }
//
//                            // 处理动作日志
//                            JSONArray actionArr = jsonObj.getJSONArray("actions");
//                            if (actionArr != null && actionArr.size() > 0) {
//                                for (int i = 0; i < actionArr.size(); i++) {
//                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
//                                    // 创建新的JSONObject，包含公共信息、页面信息和动作信息
//                                    JSONObject newActionJsonObj = new JSONObject();
//                                    newActionJsonObj.put("common", commonJsonObj);
//                                    newActionJsonObj.put("page", pageJsonObj);
//                                    newActionJsonObj.put("action", actionJsonObj);
//                                    // 将动作日志通过侧输出流输出
//                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
//                                }
//                                // 从原始JSON对象中移除动作日志字段
//                                jsonObj.remove("actions");
//                            }
//
//                            // 将处理后的页面日志收集到主输出流
//                            out.collect(jsonObj.toJSONString());
//                        }
//                    }
//                }
//        );
//
//        // 获取不同类型日志的侧输出流
//        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
//        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
//        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
//        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
//        // 将不同类型的日志打印到控制台，方便调试，分别加上相应的前缀标识
//        pageDS.print("页面:");
//        errDS.print("错误:");
//        startDS.print("启动:");
//        displayDS.print("曝光:");
//        actionDS.print("动作:");
//
//        // 创建一个Map，用于存储不同类型日志的数据流
//        Map<String, DataStream<String>> streamMap = new HashMap<>();
//        streamMap.put(ERR, errDS);
//        streamMap.put(START, startDS);
//        streamMap.put(DISPLAY, displayDS);
//        streamMap.put(ACTION, actionDS);
//        streamMap.put(PAGE, pageDS);
//
//        // 将不同类型的日志数据流分别写入对应的Kafka主题
//        streamMap
//                .get(PAGE)
//                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
//        streamMap
//                .get(ERR)
//                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
//        streamMap
//                .get(START)
//                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
//        streamMap
//                .get(DISPLAY)
//                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
//        streamMap
//                .get(ACTION)
//                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
//
//        // 执行Flink作业，作业名称为"dwd_log"
//        env.execute("dwd_log");
//    }



















    private static final String START = "start";
    private static final String ERR = "err";
    private static final String DISPLAY = "display";
    private static final String ACTION = "action";
    private static final String PAGE = "page";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_LOG, "dwd_log");

        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

//        kafkaStrDS.print();

        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );

//        jsonObjDS.print();

        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);

        dirtyDS.sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));


        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);

                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());

                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
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
//        fixedDS.print();

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
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            //~~~页面日志~~~
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
                            //~~~曝光日志~~~
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", dispalyJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            //~~~动作日志~~~
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

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

        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR,errDS);
        streamMap.put(START,startDS);
        streamMap.put(DISPLAY,displayDS);
        streamMap.put(ACTION,actionDS);
        streamMap.put(PAGE,pageDS);

        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

        env.execute("dwd_log");




    }



}
