package realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.common.base.BaseApp;
import realtime.common.bean.TrafficPageViewBean;
import realtime.common.constant.Constant;
import realtime.common.util.DateFormatUtil;
import realtime.common.util.FlinkSinkUtil;

import java.time.Duration;

/**
 * @Package realtime.dws.app.DwsTrafficVcChArIsNewPageViewWindow
 * @Author zhaohua.liu
 * @Date 2025/4/22.13:37
 * @description:
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindow().start(20013,4,"dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStreamDS) {
        //流中数据转jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStreamDS.map(jsonStr -> JSON.parseObject(jsonStr));
        //按照mid对流中数据进行分组（计算UV）
        KeyedStream<JSONObject, String> keyMidDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //再次对流中数据进行类型转换  jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = keyMidDS.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
            ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                //状态的生存时间
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                //记录上一次登录日期
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public TrafficPageViewBean map(JSONObject jsonObject) throws Exception {
                JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                JSONObject pageJsonObj = jsonObject.getJSONObject("page");

                //从状态中获取上次登录日期
                String lastVisitDate = lastVisitDateState.value();
                //获取当天日期
                Long ts = jsonObject.getLong("ts");
                String curVisitDate = DateFormatUtil.tsToDate(ts);
                //假设当天未登录
                Long uvCt = 0L;
                //如果状态中没有上次登录日期,或者上次登录日期不等于当天日期,uv设为1,更新状态为今日
                if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                    uvCt = 1L;
                    lastVisitDateState.update(curVisitDate);
                }
                //获取上一个页面id,如果为null,那么为新的一次会话
                String lastPageId = pageJsonObj.getString("last_page_id");
                long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;
                return TrafficPageViewBean.builder()
                        .stt("")
                        .edt("")
                        .cur_date("")
                        .vc(commonJsonObj.getString("vc"))
                        .ch(commonJsonObj.getString("ch"))
                        .ar(commonJsonObj.getString("ar"))
                        .isNew(commonJsonObj.getString("is_new"))
                        .uvCt(uvCt)
                        .svCt(svCt)
                        .pvCt(1L)
                        .durSum(pageJsonObj.getLong("during_time"))
                        .ts(ts)
                        .build();
            }
        });
        //指定ts字段为水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                                        return trafficPageViewBean.getTs();
                                    }
                                }
                        )
        );

        //分组,按照统计的维度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyDimDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                        return Tuple4.of(
                                trafficPageViewBean.getVc(),
                                trafficPageViewBean.getCh(),
                                trafficPageViewBean.getAr(),
                                trafficPageViewBean.getIsNew());
                    }
                }
        );


        //开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = keyDimDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(
                        //使用 ReduceFunction 对窗口内的数据进行聚合操作
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean t1, TrafficPageViewBean t2) throws Exception {
                                t1.setPvCt(t1.getPvCt() + t2.getPvCt());
                                t1.setUvCt(t1.getUvCt() + t2.getUvCt());
                                t1.setSvCt(t1.getSvCt() + t2.getSvCt());
                                t1.setDurSum(t1.getDurSum() + t2.getDurSum());
                                return t1;
                            }

                        },
                        // WindowFunction 将聚合结果与窗口的开始时间、结束时间以及日期信息关联起来
                        new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                                //由于 ReduceFunction 已经对窗口内的数据进行了聚合，所以 input 中通常只有一个元素，这里获取该元素
                                TrafficPageViewBean pageViewBean = iterable.iterator().next();
                                String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                                String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                                String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                                pageViewBean.setStt(stt);
                                pageViewBean.setEdt(edt);
                                pageViewBean.setCur_date(curDate);
                                collector.collect(pageViewBean);

                            }
                        }
                );

        //实体类转jsonObject,写入doris
        // kafka-consumer-groups --bootstrap-server cdh01:9092 --group dws_traffic_vc_ch_ar_is_new_page_view_window --reset-offsets --all-topics --to-earliest --execute
        reduceDS.map(trafficPageViewBean -> JSON.toJSONString(trafficPageViewBean))
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }
}
