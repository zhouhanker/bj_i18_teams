package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.CheckPointUtils;
import com.rb.utils.DateFormatUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.rb.dws.DwsNewPagViewWindow
 * @Author runbo.zhang
 * @Date 2025/4/28 17:43
 * @description:
 */
public class DwsNewPagViewWindow {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.setCk(env);
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "log_topic_flink_online_v2_log_page");

        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaRead.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject s = null;
                try {
                    s = JSON.parseObject(value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return s;
            }
        });
//        jsonDs.print();
        KeyedStream<JSONObject, String> keyedByMidStream = jsonDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> mapData = keyedByMidStream.map(
                new RichMapFunction<JSONObject, JSONObject>() {
            ValueState<String> lastVisitState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("String", String.class);
                lastVisitState = getRuntimeContext().getState(valueStateDescriptor);

            }
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                String lastDate = lastVisitState.value();
                Long ts = jsonObject.getLong("ts");
                String date = DateFormatUtil.tsToDate(ts);
                long uvCt = 0L;
                if (StringUtils.isEmpty(lastDate) || !lastDate.equals(date)) {
                    uvCt = 1L;
                    lastVisitState.update(date);
                }
                Long svCt=0L;
                try {
                    String lastPageId = page.getString("last_page_id");
                    if(StringUtils.isEmpty(lastPageId)){
                        svCt = 1L;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }


                jsonObject.put("uvCt", uvCt);
                jsonObject.put("svCt", svCt);
                jsonObject.put("pvCt", 1L);
                return jsonObject;
            }
        });
//        mapData.print();
        //设置水位线
        SingleOutputStreamOperator<JSONObject> watermarksDs = mapData.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration. ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));
        //根据ar地区  ch渠道 vc版本 is_new 新用户分组
        KeyedStream<JSONObject, Tuple4<String, String, String, String>> keyedStream = watermarksDs.keyBy(new KeySelector<JSONObject, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(JSONObject value) throws Exception {
                JSONObject common = value.getJSONObject("common");
                String ar = common.getString("ar");
                String ch = common.getString("ch");
                String isNew = common.getString("is_new");
                String vc = common.getString("vc");

                return Tuple4.of(ar, ch, vc, isNew);
            }
        });
//        keyedStream.print();
        SingleOutputStreamOperator<JSONObject> reduceDs = keyedStream.window(
                        TumblingEventTimeWindows
                                .of(Time.seconds(10)))
                .reduce(new ReduceFunction<JSONObject>() {
                            @Override
                            public JSONObject reduce(JSONObject v1, JSONObject v2) throws Exception {
                                Long v1_dt = v1.getJSONObject("page").getLong("during_time");
                                Long v2_dt = v2.getJSONObject("page").getLong("during_time");
                                JSONObject r1 = new JSONObject();

                                v1.put("DurSum", v1_dt + v2_dt);
                                v1.put("pvCt", v1.getLong("pvCt") + v2.getLong("pvCt"));
                                v1.put("uvCt", v1.getLong("uvCt") + v2.getLong("uvCt"));
                                r1.put("svCt", v1.getLong("svCt") + v2.getLong("svCt"));
                                JSONObject common = v1.getJSONObject("common");
                                String ar = common.getString("ar");
                                String ch = common.getString("ch");
                                String isNew = common.getString("is_new");
                                String vc = common.getString("vc");
                                v1.put("ar", ar);
                                v1.put("ch", ch);
                                v1.put("is_new", isNew);
                                v1.put("vc", vc);
//                                v1.put("v1", "111");
                                v1.remove("displays");
                                v1.remove("page");
                                v1.remove("displays");


                                return v1;
                            }
                        }, new WindowFunction<JSONObject, JSONObject, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<JSONObject> input, Collector<JSONObject> out) throws Exception {
                                JSONObject json = input.iterator().next();
                                System.out.println("aaaa->"+json);
                                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                                String curDate = DateFormatUtil.tsToDate(window.getStart());
                                String edt = DateFormatUtil.tsToDateTime(window.getEnd());


                                json.put("curDate", curDate);
                                json.put("stt", stt);
                                json.put("edt", edt);
                                out.collect(json);
                            }
                        }
                );
        reduceDs.print("reduce->");


        env.execute();
    }
}
