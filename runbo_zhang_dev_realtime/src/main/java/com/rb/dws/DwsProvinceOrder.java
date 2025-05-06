package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.fuction.DimAsync;
import com.rb.utils.DateFormatUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.rb.dwd.dwsProvinceOrder
 * @Author runbo.zhang
 * @Date 2025/4/23 15:27
 * @description:
 */
public class DwsProvinceOrder {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/DwsSkuOrder_p1");
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "dwd_trade_order_detail_v1");
        SingleOutputStreamOperator<JSONObject> mappedDs = kafkaRead.map(o -> {
            try {
                JSONObject obj = JSON.parseObject(o);
                return obj;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });


        //TODO 2.按照唯一键(订单明细的id)进行分组
        //TODO 3.去重
        SingleOutputStreamOperator<JSONObject> distinctDs = mappedDs.keyBy(o -> o.getString("id")).process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> descriptor =
                        new ValueStateDescriptor<JSONObject>(
                                "average", JSONObject.class); // default value of the state, if nothing was set

                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject data = valueState.value();
                if (data != null) {
                    String splitOriginalAmount = data.getString("split_original_amount");
                    String splitCouponAmount = data.getString("split_coupon_amount");
                    String splitActivityAmount = data.getString("split_activity_amount");
                    String splitTotalAmount = data.getString("split_total_amount");

                    data.put("split_original_amount", "-" + splitOriginalAmount);
                    data.put("split_coupon_amount", "-" + splitCouponAmount);
                    data.put("split_activity_amount", "-" + splitActivityAmount);
                    data.put("split_total_amount", "-" + splitTotalAmount);
                    out.collect(data);
                }
                valueState.update(value);
                out.collect(value);
            }
        });
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> wateredDs = distinctDs
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts_ms");
                                    }
                                }));
        //TODO 6.分组
        WindowedStream<JSONObject, String, TimeWindow> windowDs = wateredDs.keyBy(o -> o.getString("province_id"))
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 7.开窗
        //TODO 8.聚合
        SingleOutputStreamOperator<JSONObject> reduceDs =
                windowDs.reduce(new ReduceFunction<JSONObject>() {

                    HashSet<String> set =new HashSet<>();
                                    @Override
                                    public JSONObject reduce(JSONObject v1, JSONObject v2) throws Exception {
                                        v1.put("reduce_total_amount", v1.getDouble("split_total_amount") + v2.getDouble("split_total_amount"));

                                        set.add(v1.getString("order_id"));
                                        set.add(v2.getString("order_id"));
                                        v1.put("order_id_set", set);

                                        return v1;
                                    }
                                }
                                , new WindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                                    @Override
                                    public void apply(String s, TimeWindow window, Iterable<JSONObject> input, Collector<JSONObject> out) throws Exception {
                                        JSONObject next = input.iterator().next();
                                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                                        next.put("stt", stt);
                                        next.put("edt", edt);
                                        next.put("curDate", curDate);
                                        HashSet object = next.getObject("order_id_set", HashSet.class);

                                        if (object!=null){
//                                            System.out.println(object);
                                            next.put("order_count", object.size());
                                        }else {
                                            next.put("order_count", 0);
                                        }
                                        next.remove("order_id_set");
                                        out.collect(next);
                                    }
                                }
                );

        //reduceDS.print();
        //TODO 9.关联省份维度

        DataStream<JSONObject> resultStream =
                AsyncDataStream.unorderedWait(reduceDs, new DimAsync<JSONObject>() {
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
                }, 100, TimeUnit.SECONDS);
        resultStream.print();
        //TODO 10.将关联的结果写到Doris中

        resultStream.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.getDorisSink("doris_database_v1", "dws_trade_province_order_window"));
        env.disableOperatorChaining();
        env.execute();

    }
}
