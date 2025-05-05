package realtime.dwd.log.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import realtime.common.base.BaseApp;
import realtime.common.constant.Constant;
import realtime.common.util.DateFormatUtil;
import realtime.common.util.FlinkSinkUtil;

import java.util.HashMap;

/**
 * @Package realtime.dwd.log.split.DwdBaseLog
 * @Author zhaohua.liu
 * @Date 2025/4/14.11:53
 * @description: log数据分流
 */
public class    DwdBaseLog extends BaseApp {
    public static final String START =  "start";
    public static final String ERR = "err";
    public static final String DISPLAY = "display";
    public static final String ACTION = "action";
    public static final String PAGE = "page";

    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(20003,4,"dwd_base_log", Constant.TOPIC_LOG);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStreamDS) {
        //对流中数据类型进行转换  并做简单的ETL
        SingleOutputStreamOperator<JSONObject> JsonObjDS = etl(kafkaStreamDS);
        //对新老访客标记进行修复
        SingleOutputStreamOperator<JSONObject> fixedNewAndOldDS = fixedNewAndOld(JsonObjDS);
        //分流   错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流     页面日志-主流
        HashMap<String, DataStream<String>> streamMap = splitStream(fixedNewAndOldDS);
        //将数据写入不同的kafka
        writeToKafka(streamMap);

    }

    //todo writeToKafka方法
    private static void writeToKafka(HashMap<String, DataStream<String>> streamMap){
        streamMap.get(ERR).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap.get(START).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap.get(DISPLAY).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAYS));
        streamMap.get(ACTION).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTIONS));
        streamMap.get(PAGE).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
    }

    //todo splitStream方法
    private static HashMap<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixedNewAndOldDS){
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        SingleOutputStreamOperator<String> pageDS = fixedNewAndOldDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        //报错日志
                        if (jsonObject.getJSONObject("err") != null) {
                            context.output(errTag, jsonObject.toJSONString());
                            jsonObject.remove("err");
                        }
                        //启动日志
                        if (jsonObject.getJSONObject("start") != null) {
                            context.output(startTag, jsonObject.toJSONString());
                        } else {
                            //否则为页面日志
                            JSONObject common = jsonObject.getJSONObject("common");
                            JSONObject page = jsonObject.getJSONObject("page");
                            Long ts = jsonObject.getLong("ts");

                            //曝光日志
                            JSONArray displaysArr = jsonObject.getJSONArray("displays");
                            if (displaysArr != null && displaysArr.size() > 0) {
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject display = displaysArr.getJSONObject(i);
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", common);
                                    newDisplayJsonObj.put("page", page);
                                    newDisplayJsonObj.put("display", display);
                                    newDisplayJsonObj.put("ts", ts);
                                    context.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObject.remove("displays");

                            }

                            //动作日志
                            JSONArray actionsArr = jsonObject.getJSONArray("actions");
                            if (actionsArr != null && actionsArr.size() > 0) {
                                for (int i = 0; i < actionsArr.size(); i++) {
                                    JSONObject action = actionsArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", common);
                                    newActionJsonObj.put("page", page);
                                    newActionJsonObj.put("action", action);
                                    context.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }
                            //剔除err start displays actions后的页面数据,写入主流
                            collector.collect(jsonObject.toJSONString());
                        }
                    }
                }
        );
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面");
        errDS.print("报错");
        startDS.print("启动");
        displayDS.print("曝光");
        actionDS.print("活动");

        HashMap<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR,errDS);
        streamMap.put(START,startDS);
        streamMap.put(DISPLAY,displayDS);
        streamMap.put(ACTION,actionDS);
        streamMap.put(PAGE,pageDS);
        return streamMap;
    }


    //todo fixedNewAndOld方法
    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS){
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    //open创建valueState状态,设置10秒生存周期,且每当状态被创建或写入时，TTL 会被重置为初始值
                    private ValueState<String> lastVisitDataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDataState", String.class);
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig
                                        .newBuilder(Time.seconds(10))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        lastVisitDataState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    //新老用户校验
                    //如果is_new的值为1
                    //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不修改is_new 字段
                    //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                    //如果 is_new 的值为 0
                    //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
                    // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        String lastVisitDate = lastVisitDataState.value();

                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDataState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    jsonObj.getJSONObject("common").put("is_new", "0");
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDataState.update(yesterday);
                            }
                        }
                        return jsonObj;
                    }
                }
        );
        fixedDS.print();
        return fixedDS;
    }


    //todo etl方法
    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS){
        //定义测流
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
        //etl处理
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    //如果转换的时候，没有发生异常，说明是标准的json，将数据传递的下游
                    JSONObject jsonObj = JSON.parseObject(s);
                    collector.collect(jsonObj);
                } catch (Exception e) {
                    //如果转换的时候，发生了异常，说明不是是标准的json，属于脏数据，将其放到侧输出流中
                    context.output(dirtyTag, s);
                }
            }
        });

        //将脏数据写入kafka
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        KafkaSink<String> dirtyDataSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(dirtyDataSink);
        //返回主流
        return jsonObjDS;
    }
}
