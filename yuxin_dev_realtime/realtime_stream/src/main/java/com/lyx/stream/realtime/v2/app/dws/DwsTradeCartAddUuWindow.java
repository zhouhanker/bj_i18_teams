package com.lyx.stream.realtime.v2.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.lyx.stream.realtime.v2.app.bean.CartAddUuBean;
import com.lyx.stream.realtime.v2.app.function.BeanToJsonStrMapFunction;
import com.lyx.stream.realtime.v2.app.utils.DateFormatUtil;
import com.lyx.stream.realtime.v2.app.utils.FlinkSinkUtil;
import com.lyx.stream.realtime.v2.app.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.lyx.stream.realtime.v2.app.dws.DwsTradeCartAddUuWindow
 * @Author yuxin_li
 * @Date 2025/4/21 9:36
 * @description: DwsTradeCartAddUuWindow
 * 从 Kafka 读取购物车加购数据
 * 进行去重、窗口聚合等处理，最终将处理结果写入到 Doris 数据库的功能
 */

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        //环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        //设置重启策略为固定延迟重启，最多尝试 3 次，每次重启间隔 3000 毫秒（即 3 秒），以应对作业执行过程中可能出现的错误或失败情况
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        //从 Kafka 读取数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_cart_add_yuxin_li", "dws_trade_cart_add_uu_window");

        //将 Kafka 数据源添加到 Flink 环境中，创建一个字符串类型的数据流 kafkaStrDS
        // 并设置不使用水位线 表明假设数据是有序的，不需要处理乱序数据的情况
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        //数据转换为 JSON 对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        //分配时间戳和水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms") * 1000;
                                    }
                                }
                        )
        );

        //按键分区 将相同 user_id 的数据分配到同一个分区中进行处理，便于后续针对每个用户进行独立的状态管理和计算
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        //去重处理
        //状态的存活时间设置为 1 天
        // 在处理每个元素时，获取当前加购的日期并与上次加购日期比较，
        // 如果日期不同，则将该数据输出（即认为是一次新的独立加购），并更新状态
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        //获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts_ms");
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );

        //窗口操作
        AllWindowedStream<JSONObject, TimeWindow> windowDS = cartUUDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(2)));


        //窗口聚合
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
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
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) {
                        Long cartUUCt = values.iterator().next();
                        long startTs = window.getStart() / 1000;
                        long endTs = window.getEnd() / 1000;
                        String stt = DateFormatUtil.tsToDateTime(startTs);
                        String edt = DateFormatUtil.tsToDateTime(endTs);
                        String curDate = DateFormatUtil.tsToDate(startTs);
                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );

        //数据转换为 JSON 字符串
        SingleOutputStreamOperator<String> operator = aggregateDS
                .map(new BeanToJsonStrMapFunction<>());

        operator.print();

        operator.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

        env.execute("DwsTradeCartAddUuWindow");
    }
}
