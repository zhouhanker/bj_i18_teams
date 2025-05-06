package com.zpy.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zpy.constant.Constant;
import com.zpy.utils.DateFormatUtil;
import com.zpy.utils.FlinkSinkUtil;
import com.zpy.utils.FlinkSourceUtil;
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
 * DWD层基础日志处理程序
 * 功能：处理原始日志数据，进行数据清洗、分流和标准化处理
 * 主要处理流程：
 * 1. 从Kafka读取原始日志数据
 * 2. 数据清洗和格式校验
 * 3. 用户访问状态修复（is_new字段修正）
 * 4. 日志数据分流（页面、启动、错误、曝光、动作）
 * 5. 将不同业务类型的数据写入不同的Kafka主题
 */
public class DwdBaseLog {

    // 定义日志类型常量
    private static final String START = "start";    // 启动日志
    private static final String ERR = "err";       // 错误日志
    private static final String DISPLAY = "display"; // 曝光日志
    private static final String ACTION = "action";   // 动作日志
    private static final String PAGE = "page";      // 页面日志

    public static void main(String[] args) throws Exception {
        // 1. 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为4
        env.setParallelism(4);

        // 启用检查点，每5秒一次，使用EXACTLY_ONCE语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 2. 从Kafka读取原始日志数据
        // 使用工具类获取Kafka源，主题为Constant.TOPIC_LOG，消费者组为dwd_log
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_LOG, "dwd_log");

        // 创建Kafka数据流，不设置水位线
        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 3. 数据清洗和格式校验
        // 定义脏数据侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        // 处理原始JSON字符串，输出JSONObject，非法JSON输出到侧输出流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        try {
                            // 尝试解析JSON
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            // 解析失败的数据输出到侧输出流
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );

        // 4. 处理脏数据
        // 获取脏数据流
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        // 将脏数据写入专门的Kafka主题

        dirtyDS.sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));

        // 5. 用户访问状态修复
        // 按照设备ID(mid)分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // 使用状态修复is_new字段
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState; // 保存用户最后访问日期的状态

                    @Override
                    public void open(Configuration parameters) {
                        // 配置状态描述符
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);

                        // 设置状态TTL为10秒（状态存活时间）
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());

                        // 获取状态
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            // 如果是新用户标记
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                // 如果状态中没有记录，更新状态
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    // 如果上次访问日期与当前不同，修正is_new为0
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            // 对于非新用户，如果状态为空，设置为昨天日期
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }

                        return jsonObj;
                    }
                }
        );

        // 6. 日志数据分流
        // 定义各种日志类型的侧输出流标签 利用侧输出流实现数据拆分
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};

        // 分流处理
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) {
                        // 处理错误日志
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        // 处理启动日志
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            // 处理页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            // 处理曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
                                    // 构建新的曝光日志对象
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", dispalyJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            // 处理动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    // 构建新的动作日志对象
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            // 输出页面日志
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );

        // 7. 获取各分流数据
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);    // 错误日志流
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag); // 启动日志流
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag); // 曝光日志流
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);   // 动作日志流

        // 打印各流数据（调试用）
//        本节将按照内容，将日志分为以下五类
        //启动日志
        //页面日志
        //曝光日志
        //动作日志
        //错误日志`
        pageDS.print("页面:");
        errDS.print("错误:");
        startDS.print("启动:");
        displayDS.print("曝光:");
        actionDS.print("动作:");

        // 8. 将各流数据写入Kafka不同主题
        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR, errDS);      // 错误日志
        streamMap.put(START, startDS);   // 启动日志
        streamMap.put(DISPLAY, displayDS); // 曝光日志
        streamMap.put(ACTION, actionDS);   // 动作日志
        streamMap.put(PAGE, pageDS);      // 页面日zhi

        // 将各流写入对应的Kafka主题
        streamMap.get(PAGE).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap.get(ERR).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap.get(START).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap.get(DISPLAY).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap.get(ACTION).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

        // 9. 执行作业
        env.execute("dwd_log");
    }
}