package com.dwd.ealtime_dwd_base_log;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.common.base.BaseApp;
import com.common.utils.Constat;
import com.common.utils.DateFormatUtil;
import com.common.utils.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
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
/*
需要启动的进程
1、
 */
public class DwdBaseLog extends BaseApp {
    private static String brokers = "cdh02:9092";
    public static void main(String[] args) {
        new DwdBaseLog().start(10002,1,"DwdBaseLog", Constat.TOPIC_LOG,"source_log");
    }

    @Override
    public void Handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceDS) {
        //TODO 对流中数据进行转换并做简单的ETL转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaSourceDS);
        //TODO 对新老访客标记进行修复
        SingleOutputStreamOperator<JSONObject> filedDS = fixedNewAndOld(jsonObjDS);
        //TODO 错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流页面日志-主流 
        //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        SingleOutputStreamOperator<String> pageDs = filedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        //错误日志
                        JSONObject errjsonObj = jsonObj.getJSONObject("err");

                        if (errjsonObj != null) {
                            //讲错物流写入测流中
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }
                        //启动日志
                        JSONObject startjsonObj = jsonObj.getJSONObject("start");
                        if (startjsonObj != null) {
                            //启动
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            //页面
                            JSONObject commonjsonObj = jsonObj.getJSONObject("common");
                            JSONObject pagejsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
                            //曝光
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                //遍历出当前页面的所有曝光信息
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJSONObj = displayArr.getJSONObject(i);
                                    //定义一个新的JSON对象，用于封装遍历出来的曝光数据
                                    JSONObject newDisplayObj = new JSONObject();
                                    newDisplayObj.put("common", commonjsonObj);
                                    newDisplayObj.put("page", pagejsonObj);
                                    newDisplayObj.put("display", displayJSONObj);
                                    newDisplayObj.put("ts", ts);
                                    //
                                    //将曝光日志写到曝光侧输出流
                                    ctx.output(displayTag, newDisplayObj.toJSONString());
                                }
                            }
                            //动作
                            JSONArray actionsArr = jsonObj.getJSONArray("actions");
                            if (actionsArr != null && actionsArr.size() > 0) {
                                for (int i = 0; i < actionsArr.size(); i++) {
                                    JSONObject actionJSONObj = actionsArr.getJSONObject(i);
                                    JSONObject newActionObj = new JSONObject();
                                    newActionObj.put("common", commonjsonObj);
                                    newActionObj.put("page", pagejsonObj);
                                    newActionObj.put("action", actionJSONObj);
                                    ctx.output(actionTag, newActionObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            //页面放主流
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );
        SideOutputDataStream<String> errDs = pageDs.getSideOutput(errTag);
        SideOutputDataStream<String> startDs = pageDs.getSideOutput(startTag);
        SideOutputDataStream<String> displayDs = pageDs.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDs = pageDs.getSideOutput(actionTag);
        pageDs.print("页面：");
        errDs.print("错误：");
        startDs.print("启动：");
        displayDs.print("曝光：");
        actionDs.print("动作：");
        //TODO 将不同流的数据写入KAFKA主题中
        pageDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constat.TOPIC_DWD_TRAFFIC_PAGE));
        errDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constat.TOPIC_DWD_TRAFFIC_ERR));
        startDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constat.TOPIC_DWD_TRAFFIC_START));
        displayDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constat.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constat.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {

        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //使用Flink的状态变成完成修fu 新老用户
        SingleOutputStreamOperator<JSONObject> filedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取is_new值
                        String is_new = jsonObj.getJSONObject("common").getString("is_new");
                        //从监控状态中拿去首次日期
                        String lastVisiDate = lastVisitDateState.value();
                        //获取当前访问日期

                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(is_new)) {
                            if (StringUtils.isEmpty(lastVisiDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisiDate.equals(curVisitDate)) {
                                    is_new = "0";
                                    jsonObj.getJSONObject("common").put("is_new", is_new);
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisiDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }
                        return jsonObj;
                    }
                }
        );
//        filedDS.print();
        return filedDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSourceDS) {

        //定义侧输出流
        //ETL
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            //如果转换的时候没有异常，说明是标准的json间数据传递到下游
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            //如果转换的时候发生异常，属于脏数据，将其放到侧输出流中
                            context.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
//        jsonObjDS.print("标准的json");
        SideOutputDataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyTag);
//        sideOutput.print("脏数据");

        //将侧输出流的脏数据写入kafka主题中
        KafkaSink<String> dirtyData = FlinkSinkUtil.getKafkaSink("dirty_data");
        sideOutput.sinkTo(dirtyData);
        return jsonObjDS;
    }
}
