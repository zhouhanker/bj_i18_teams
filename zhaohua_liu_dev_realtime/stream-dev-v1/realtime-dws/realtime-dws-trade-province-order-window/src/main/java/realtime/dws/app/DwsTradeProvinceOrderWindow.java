package realtime.dws.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.common.base.BaseApp;
import realtime.common.constant.Constant;
import realtime.common.function.DimAsyncFunction;
import realtime.common.util.DateFormatUtil;
import realtime.common.util.FlinkSinkUtil;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Package realtime.dws.app.DwsTradeProvinceOrderWindow
 * @Author zhaohua.liu
 * @Date 2025/4/25.22:10
 * @description:
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindow().start(
            20019,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStreamDS) {
        //过滤空消息
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStreamDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        if (s != null && !s.trim().isEmpty()) {
                            collector.collect(JSON.parseObject(s));
                        }
                    }
                }
        );

        //按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> keySkuIdDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("id"));

        //去重
        SingleOutputStreamOperator<JSONObject> distinctDS = keySkuIdDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> lastJsonState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> lastJsonStateDescriptor = new ValueStateDescriptor<JSONObject>("lastJsonStateDescriptor", JSONObject.class);
                        lastJsonStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonState = getRuntimeContext().getState(lastJsonStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject lastJson = lastJsonState.value();
                        if (lastJson != null) {
                            lastJson.put("split_total_amount", "-" + lastJson.getString("split_total_amount"));
                            collector.collect(lastJson);
                        }
                        lastJsonState.update(jsonObject);
                        collector.collect(jsonObject);
                    }
                }
        );

        //设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        )
        );

        //提取需要的字段到新的json
        SingleOutputStreamOperator<JSONObject> newjsonDS = withWatermarkDS.map(
                new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        String provinceId = jsonObj.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts");
                        String orderId = jsonObj.getString("order_id");


                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("province_id", provinceId);
                        jsonObject.put("split_total_amount", splitTotalAmount);
                        jsonObject.put("ts", ts);
                        jsonObject.put("order_id", orderId);
                        return jsonObject;
                    }
                }
        );

        //按省份分组
        KeyedStream<JSONObject, String> keyProvinceIdDS = newjsonDS.keyBy(jsonObject -> jsonObject.getString("province_id"));

        //开窗聚合

        SingleOutputStreamOperator<JSONObject> aggregateDS = keyProvinceIdDS
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .aggregate(new AggregateFunction<JSONObject, Tuple2<BigDecimal, HashSet<String>>, JSONObject>() {
                               @Override
                               public Tuple2<BigDecimal, HashSet<String>> createAccumulator() {
                                   return Tuple2.of(BigDecimal.ZERO, new HashSet<>());
                               }

                               @Override
                               public Tuple2<BigDecimal, HashSet<String>> add(JSONObject jsonObj, Tuple2<BigDecimal, HashSet<String>> acc) {
                                   acc.f0 = acc.f0.add(jsonObj.getBigDecimal("split_total_amount"));
                                   acc.f1.add(jsonObj.getString("order_id"));
                                   return acc;
                               }

                               @Override
                               public JSONObject getResult(Tuple2<BigDecimal, HashSet<String>> acc) {
                                   JSONObject jsonObj = new JSONObject();
                                   jsonObj.put("split_total_amount", acc.f0);
                                   jsonObj.put("order_count", acc.f1.size());
                                   return jsonObj;
                               }

                               @Override
                               public Tuple2<BigDecimal, HashSet<String>> merge(Tuple2<BigDecimal, HashSet<String>> acc1, Tuple2<BigDecimal, HashSet<String>> acc2) {
                                   acc1.f0 = acc1.f0.add(acc2.f0);
                                   acc1.f1.addAll(acc2.f1);
                                   return acc1;
                               }
                           },
                        new WindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow timeWindow, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
                                JSONObject next = iterable.iterator().next();
                                String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                                String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                                String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                                next.put("stt", stt);
                                next.put("edt", edt);
                                next.put("cur_date", curDate);
                                next.put("province_id",s);
                                collector.collect(next);
                            }
                        });

        //关联省份维度
        SingleOutputStreamOperator<JSONObject> withProvinceNameDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        obj.put("province_name", dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("province_id");
                    }
                }, 60, TimeUnit.SECONDS
        );
        withProvinceNameDS.print();
        withProvinceNameDS
                .map(jsonObject -> JSONObject.toJSONString(jsonObject))
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}
