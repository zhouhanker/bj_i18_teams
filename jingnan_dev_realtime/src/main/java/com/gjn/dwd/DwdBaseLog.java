package com.gjn.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gjn.constant.Constant;
import com.gjn.util.DateFormatUtil;
import com.gjn.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package com.gjn.dwd.DwdBaseLog
 * @Author jingnan.guo
 * @Date 2025/4/15 10:57
 * @description:
 */
public class DwdBaseLog{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("topic_log")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        kafkaStrDS.print();



        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            context.output(dirtyTag, s);
                        }
                    }
                }
        );
        //jsonObjDS.print(">>>>标准的json:");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        //dirtyDS.print(">>>>>脏数据:");

        //存储脏数据的topic
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dirty_data")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        dirtyDS.sinkTo(kafkaSink);

        //判断新老用户
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(o -> o.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> fixedDs = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("in_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }
                        return jsonObject;
                    }
                });
        fixedDs.print(">>>>>分流");

        //测流输出 错误 启动 曝光 动作
        OutputTag<String> errTag = new OutputTag<String>("errTag"){};
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag"){};
        SingleOutputStreamOperator<String> pageDS = fixedDs.process(
                new ProcessFunction<JSONObject, String>() {

                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toString());
                            jsonObj.remove("err");
                        }
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null){
                            ctx.output(startTag, jsonObj.toString());
                        } else {
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
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
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        env.execute();

    }
}
