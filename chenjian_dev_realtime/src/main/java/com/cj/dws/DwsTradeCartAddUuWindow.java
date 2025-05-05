package com.cj.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.bean.CartADDUU;
import com.cj.utils.dataformtutil;
import com.cj.utils.FlinkSinkUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.cj.dws.DwsTradeCartAddUuWindow
 * @Author chen.jian
 * @Date 2025/4/15 11:47
 * @description: 加购独立用户统计
 */
public class DwsTradeCartAddUuWindow {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        设置了检查点的超时时间为 60000 毫秒（即 60 秒）。如果在 60 秒内检查点操作没有完成，就会被视为失败。
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        当作业被取消时，检查点数据不会被删除，而是会保留下来，这样在后续需要时可以利用这些检查点数据进行恢复操作。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        两次检查点操作之间的最小间隔时间为 2000 毫秒（即 2 秒）。这是为了避免在短时间内频繁进行检查点操作，从而影响作业的正常处理性能。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        表示在 30 天内允许的最大失败次数为 3 次。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
//        状态后端用于管理 Flink 作业的状态数据，HashMapStateBackend 会将状态数据存储在 TaskManager 的内存中，适用于小规模的状态管理。

//        获取kafka
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("cdh02:9092")
            .setTopics("dwd_trade_cart_add")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        转成json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
//        指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<JSONObject>() {
                        @Override
                        public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                            return jsonObj.getLong("ts") ;
                        }
                    }
                )
        );
//        withWatermarkDS.print();
//        按照用户的id进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(o -> o.getString("user_id"));
//        状态编程判断是否为加购独立用户
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        String lastCartDate = lastCartDateState.value();
                        Long ts = jsonObj.getLong("ts") ;
                        String curCartDate = dataformtutil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );
//        开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS
                = cartUUDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
//        聚合
        SingleOutputStreamOperator<CartADDUU> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartADDUU, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartADDUU> out) throws Exception {
                        Long cartUUCt = values.iterator().next();
                        String stt = dataformtutil.tsToDateTime(window.getStart());
                        String edt = dataformtutil.tsToDateTime(window.getEnd());
                        String curDate = dataformtutil.tsToDate(window.getStart());
                        out.collect(new CartADDUU(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );
        aggregateDS.print();
//       写入到doris
        aggregateDS.
                map(new MapFunction<CartADDUU, String>() {
                    @Override
                    public String map(CartADDUU cartADDUU) throws Exception {
                        return JSON.toJSONString(cartADDUU);
                    }
                }).
                sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

        env.execute();
    }
}
