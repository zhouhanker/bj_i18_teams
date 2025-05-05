package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.DateFormatUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.rb.dws.DwsCartUU
 * @Author runbo.zhang
 * @Date 2025/4/14 10:25
 * @description:
 */
public class DwsCartUU {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/cartuu55");
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "dwd_cart_add");
//        kafkaRead.print();
        SingleOutputStreamOperator<JSONObject> JsonObj = kafkaRead.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> waterData = JsonObj.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        SingleOutputStreamOperator<JSONObject> uuDs = waterData.keyBy(o -> o.getString("user_id")).process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<String> laseCartDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor<>("laseCartDate", String.class);
                laseCartDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                String lastDate = laseCartDateState.value();
                Long ts = value.getLong("ts");
                String date = DateFormatUtil.tsToDate(ts);
                if (StringUtils.isEmpty(lastDate) || !date.equals(lastDate)) {
                    out.collect(value);
                    laseCartDateState.update(lastDate);
                }
            }
        });
//        waterData.print();
        AllWindowedStream<JSONObject, TimeWindow> windowDs = uuDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<String> aDs =
                windowDs.
                        aggregate(new AggregateFunction<JSONObject, Long, Long>() {
                                      @Override
                                      public Long createAccumulator() {
                                          System.out.println("createAccumulator");
                                          return 0L;
                                      }

                                      @Override
                                      public Long add(JSONObject value, Long accumulator) {

//                                          System.out.println("add");
                                          return ++accumulator;
                                      }

                                      @Override
                                      public Long getResult(Long accumulator) {
//                                          System.out.println("getResult");
                                          return accumulator;
                                      }

                                      @Override
                                      public Long merge(Long a, Long b) {
                                          return null;
                                      }
                                  }, new  AllWindowFunction<Long, String, TimeWindow>() {
                                      @Override
                                      public void apply(TimeWindow window, Iterable<Long> values, Collector<String> out) throws Exception {

//                                          System.out.println("顶顶顶顶的点点滴滴");
                                          Long cartUUCount = values.iterator().next();
                                          String start = DateFormatUtil.tsToDateTime(window.getStart());
                                          String date = DateFormatUtil.tsToDate(window.getStart());
                                          String end = DateFormatUtil.tsToDateTime(window.getEnd());
                                          JSONObject object = new JSONObject();
                                          object.put("stt", start);
                                          object.put("edt", end);
                                          object.put("cur_date", date);
                                          object.put("cart_add_uu_ct", cartUUCount);

                                          out.collect(object.toJSONString());


                                      }
                                  }
                        );
        aDs.print("45431224");
        aDs.sinkTo(SourceSinkUtils.getDorisSink("doris_database_v1", "cart_add_uu_ct_table_v2"));

        env.disableOperatorChaining();
        env.execute();

    }
}
