package dwd.log;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import base.BaseApp;
import constant.Constant;
import util.DateFormatUtil;
import util.FlinkSinkUtil;
import lombok.SneakyThrows;
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


/**
 * @Package com.cm.dwd.log.DwdBaseLog
 * @Author chen.ming
 * @Date 2025/4/10 16:25
 * @description: 日志分流 / 用户行为
 */
public class DwdBaseLog  extends BaseApp {
    @SneakyThrows
    public static void main(String[] args) {
        new DwdBaseLog().start(4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
    //TODO 对流中数据类型进行转换 并做简单的ETLI
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //如果转换的时候，没有发生异常，说明是标准的门json将数据传递的下游
                    collector.collect(jsonObj);
                } catch (Exception e) {
                    //如果转换的时候，发生异常，说明不是标准的门json 属于脏数据 放入测流
                    context.output(dirtyTag, jsonStr);
                }
            }
        });
//        jsonObjDS.print("标准数据");
//        SideOutputDataStream<String> dirtyDs = jsonObjDS.getSideOutput(dirtyTag);
//        dirtyDs.print("脏数据");
//        定义侧输出流标签
//        将侧输出流中的脏数据写到kafka主题中
//        FlinkSinkUtil.getFlinkSinkUtil("chenming_zang");
//        TODO 对新老访客标记进行修复
//        按照设备id进行分
        KeyedStream<JSONObject, String> keyedDs = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        // 使用Flink的状态编程完成修复
        SingleOutputStreamOperator<JSONObject> fiexdDs = keyedDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> listVisitDateState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("listVisitDateState", String.class);
                listVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }
            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {
                //获取is_new的值
                String isNew = jsonObj.getJSONObject("common").getString("is_new");
                //从状态中获取首次访问日期
                String listVisitDate = listVisitDateState.value();
                //获取当前访问日期
                Long ts = jsonObj.getLong("ts");
                //转化日期
                String curVisitDate = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    //如果is_new是1
                    if (StringUtils.isEmpty(listVisitDate)) {
                        //如果键控状态为null，认为本次是该访客首次访问APP，将日志中ts对应的日期更新到状态中，不对is_new字段做修改；
                        listVisitDateState.update(curVisitDate);
                    } else {
                        //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将is_new字段置为日；
                        if (!listVisitDate.equals(curVisitDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    // 如果 is_new 的值为0
                    //如果键控状态为nu飞，说明访间APP的是老访客但本次是该访客的员圆日志官次进入程。当前瑞新老访客状态标记云失时。
                    // 日志进入程序被判定为新访客，Fnk程序就可以次态际记只要将状态中的目期设置为今天之前即可。本程序选托将状态更新为昨口。
                    if (StringUtils.isEmpty(listVisitDate)) {
                        String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                        listVisitDateState.update(yesterDay);
                    }
                }
                return jsonObj;
            }
        });
//        fiexdDs.print();
//        4> {"common":{"ar":"18","uid":"4","os":"Android 12.0","ch":"360","is_new":"0","md":"xiaomi 13","mid":"mid_359","vc":"v2.1.134","ba":"xiaomi","sid":"b9b29ef1-55e9-4d8f-a536-6a9e63bf65b0"},"page":{"page_id":"activity1111","refer_id":"1","during_time":11569},"displays":[{"pos_seq":0,"item":"8","item_type":"sku_id","pos_id":8},{"pos_seq":1,"item":"26","item_type":"sku_id","pos_id":8},{"pos_seq":2,"item":"24","item_type":"sku_id","pos_id":8},{"pos_seq":3,"item":"9","item_type":"sku_id","pos_id":8},{"pos_seq":4,"item":"29","item_type":"sku_id","pos_id":8},{"pos_seq":5,"item":"30","item_type":"sku_id","pos_id":8},{"pos_seq":6,"item":"34","item_type":"sku_id","pos_id":8},{"pos_seq":7,"item":"30","item_type":"sku_id","pos_id":8},{"pos_seq":8,"item":"30","item_type":"sku_id","pos_id":8},{"pos_seq":9,"item":"3","item_type":"sku_id","pos_id":8},{"pos_seq":10,"item":"24","item_type":"sku_id","pos_id":8},{"pos_seq":11,"item":"21","item_type":"sku_id","pos_id":8},{"pos_seq":12,"item":"23","item_type":"sku_id","pos_id":8},{"pos_seq":13,"item":"7","item_type":"sku_id","pos_id":8},{"pos_seq":14,"item":"27","item_type":"sku_id","pos_id":8},{"pos_seq":15,"item":"11","item_type":"sku_id","pos_id":8},{"pos_seq":16,"item":"23","item_type":"sku_id","pos_id":8},{"pos_seq":17,"item":"34","item_type":"sku_id","pos_id":8},{"pos_seq":18,"item":"22","item_type":"sku_id","pos_id":8},{"pos_seq":19,"item":"12","item_type":"sku_id","pos_id":8},{"pos_seq":20,"item":"30","item_type":"sku_id","pos_id":8},{"pos_seq":21,"item":"30","item_type":"sku_id","pos_id":8},{"pos_seq":22,"item":"8","item_type":"sku_id","pos_id":8},{"pos_seq":0,"item":"33","item_type":"sku_id","pos_id":9},{"pos_seq":1,"item":"28","item_type":"sku_id","pos_id":9}],"ts":1744037187774}
        //TODO 分流 错误日志-错误侧输出流启动日志-启动侧输出流曝光日志-曝光侧输出流动作日志-动作侧输出流页面日志-主流
    //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>( "startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>(  "displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>( "actionTag") {};
        //分流
        SingleOutputStreamOperator<String> pageDS = fiexdDs.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        //错误日志
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            //将错误日志写入侧输出流
                            context.output(errTag, jsonObj.toString());
                            jsonObj.remove("err");
                        }
                        JSONObject startjsonObj = jsonObj.getJSONObject("start");
                        if (startjsonObj != null) {
                            //启动日志
                            context.output(startTag, jsonObj.toJSONString());
                        } else {
                            //页面日志
                            JSONObject commonjsonObj = jsonObj.getJSONObject("common");
                            JSONObject pagejsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
                            //曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                //便利当前页面的所有曝光信息
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject dispalyJSonObj = displayArr.getJSONObject(i);
                                    //定义一个新的json对象,用于封装遍历出来的 曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonjsonObj);
                                    newDisplayJsonObj.put("page", pagejsonObj);
                                    newDisplayJsonObj.put("display", dispalyJSonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    //将曝光日志写入测流
                                    context.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }
                            //动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                //遍历出每一个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    //定义一个新的json对象 用于封装动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonjsonObj);
                                    newActionJsonObj.put("page", pagejsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    //将动作日志写入测流
                                    context.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            //页面日志 写入主流
                            collector.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );
        pageDS.print("页面日志===>");
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        errDS.print("错误日志===>");
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        startDS.print("启动日志===>");
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        displayDS.print("曝光日志===>");
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        actionDS.print("动作日志===>");
        //TODO 将不同流的数据写到kafka的不同主题中
        pageDS.sinkTo(FlinkSinkUtil.getFlinkSinkUtil(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getFlinkSinkUtil(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getFlinkSinkUtil(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getFlinkSinkUtil(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getFlinkSinkUtil(Constant.TOPIC_DWD_TRAFFIC_ACTION));

    }
}
