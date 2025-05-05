package realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import realtime.common.base.BaseApp;
import realtime.common.bean.CartAddUuBean;
import realtime.common.constant.Constant;
import realtime.common.util.DateFormatUtil;
import realtime.common.util.FlinkSinkUtil;

/**
 * @Package realtime.dws.app.DwsTradeCartAddUuWindow
 * @Author zhaohua.liu
 * @Date 2025/4/23.16:18
 * @description:
 */
public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow().start(
                20016,
                1,
                "dws_trade_cart_add_uu_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStreamDS) {
        //对流中的数据类型进行转换   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStreamDS.map(jsonStr -> JSON.parseObject(jsonStr));
        //指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        )
        );
        //按照用户的id进行分组
        KeyedStream<JSONObject, String> keyUserIdDS = withWatermarkDS.keyBy(jsonObject -> jsonObject.getString("user_id"));
        //使用Flink的状态编程  判断是否为加购独立用户  这里不需要封装统计的实体类对象，直接将jsonObj传递到下游
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyUserIdDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        String lastCartDate = lastCartDateState.value();

                        Long ts = jsonObject.getLong("ts");
                        String curCartDate = DateFormatUtil.tsToDate(ts);

                        //之前加购过,且新数据为 新的一天,更新状态日期为新数据的日期,且将新数据收集传递下游
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            collector.collect(jsonObject);
                            lastCartDateState.update(curCartDate);
                        }

                    }
                }
        );
        //开窗
        AllWindowedStream<JSONObject, TimeWindow> withWindowDS = cartUUDS.windowAll(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        );
        //聚合
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = withWindowDS.aggregate(
                //第一个泛型参数 JSONObject 表示输入元素的类型。
                //第二个泛型参数 Long 表示累加器的类型。
                //第三个泛型参数 Long 表示最终聚合结果的类型。
                new AggregateFunction<JSONObject, Long, Long>() {
                    //用于创建一个初始的累加器，这里返回 0L，表示初始的计数为 0。
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    //当有新的元素 value 到达时， ++aLong 将累加器的值加 1
                    @Override
                    public Long add(JSONObject jsonObject, Long aLong) {
                        return ++aLong;
                    }

                    //从累加器中提取最终的聚合结果，这里直接返回累加器的值
                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    //用于合并两个累加器,在并行计算中需要使用
                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Long> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());

                        Long cartUUCt = iterable.iterator().next();
                        collector.collect(
                                new CartAddUuBean(
                                        stt,
                                        edt,
                                        curDate,
                                        cartUUCt
                                )
                        );
                    }
                }
        );
        //将聚合的结果写到Doris
        aggregateDS.print();
        aggregateDS.map(jsonStr->JSONObject.toJSONString(jsonStr))
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

    }
}
