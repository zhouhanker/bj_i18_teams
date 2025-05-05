package realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.common.base.BaseApp;
import realtime.common.constant.Constant;
import realtime.common.function.DimAsyncFunction;
import realtime.common.util.DateFormatUtil;
import realtime.common.util.FlinkSinkUtil;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * @Package realtime.dws.app.DwsTradeSkuOrderWindow
 * @Author zhaohua.liu
 * @Date 2025/4/23.20:43
 * @description:
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(
                20018,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    //json聚合函数
    private static JSONObject reduceJson(JSONObject t1,JSONObject t2,String... s){
        for (String s1 : s) {
            t1.put(s1,t1.getBigDecimal(s1).add(t2.getBigDecimal(s1)));
        }
        return t1;
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStreamDS) {
        //过滤空消息  并对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStreamDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        if (!StringUtils.isEmpty(s)) {
                            collector.collect(JSON.parseObject(s));
                        }
                    }
                }
        );
        //按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> keyOrderIdDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("id"));
        //去重,按order_detail_id分组,在组内使用状态+抵消的方法对重复的id去重,重复发生在dwd_trade_order_detail的左连接阶段,去重操作在度量值的聚合阶段生效
        //优点：时效性好    缺点：如果出现重复，需要向下游传递3条数据(数据膨胀)
        SingleOutputStreamOperator<JSONObject> distinctDS = keyOrderIdDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> lastJsonObjStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        lastJsonObjStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(lastJsonObjStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject,
                            JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        //从状态获取上次接受到的数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        //如果状态不为null,证明已经重复,则再补发一条将度量值取反后的数据
                        if (lastJsonObj != null) {
                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            collector.collect(lastJsonObj);
                        }
                        //收到数据后,直接发往下游,保证时效性
                        lastJsonObjState.update(jsonObject);
                        collector.collect(jsonObject);

                    }
                }
        );

        //指定水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkDS = distinctDS.assignTimestampsAndWatermarks(
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



        //提取度量值字段,写入jsonObject
        SingleOutputStreamOperator<JSONObject> newJsonDS = withWaterMarkDS.map(
                new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {

                        JSONObject newJsonObj = new JSONObject();
                        newJsonObj.put("sku_id", jsonObj.getString("sku_id"));
                        newJsonObj.put("split_original_amount", jsonObj.getBigDecimal("split_original_amount"));
                        newJsonObj.put("split_coupon_amount", jsonObj.getBigDecimal("split_coupon_amount"));
                        newJsonObj.put("split_activity_amount", jsonObj.getBigDecimal("split_activity_amount"));
                        newJsonObj.put("split_total_amount", jsonObj.getBigDecimal("split_total_amount"));
                        newJsonObj.put("ts", jsonObj.getLong("ts"));
                        return newJsonObj;
                    }
                }
        );

        //按sku_id分组,准备进行度量值聚合
        KeyedStream<JSONObject, String> keySkuIdDS = newJsonDS.keyBy(jsonObject -> jsonObject.getString("sku_id"));

        //开窗,聚合度量值
        SingleOutputStreamOperator<JSONObject> reduceDS = keySkuIdDS.window(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        ).reduce(
                new ReduceFunction<JSONObject>() {
                    @Override
                    public JSONObject reduce(JSONObject t1, JSONObject t2) throws Exception {
                        JSONObject t = reduceJson(t1, t2, "split_original_amount", "split_coupon_amount", "split_activity_amount", "split_total_amount");
                        return t;
                    }
                }, new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObj = iterable.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());

                        jsonObj.put("stt", stt);
                        jsonObj.put("edt", edt);
                        jsonObj.put("curDate", curDate);
                        collector.collect(jsonObj);

                    }
                }
        );

        //异步IO + 模板
        SingleOutputStreamOperator<JSONObject> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                //自定义的抽象工具类
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        obj.put("sku_name",dimJsonObj.getString("sku_name"));
                        obj.put("spu_id",dimJsonObj.getString("spu_id"));
                        obj.put("category3_id",dimJsonObj.getString("category3_id"));
                        obj.put("tm_id",dimJsonObj.getString("tm_id"));
                    }

                    //redis数据写入时自动建表,无需显式建表
                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("sku_id");
                    }
                }, 60, TimeUnit.SECONDS
        );

        //关联spu维度
        SingleOutputStreamOperator<JSONObject> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        obj.put("spu_name", dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("spu_id");
                    }
                }, 60, TimeUnit.SECONDS
        );

        //关联tm维度
        SingleOutputStreamOperator<JSONObject> withTmInfoDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        obj.put("tm_name", dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("tm_id");
                    }
                }, 60, TimeUnit.SECONDS
        );

        //关联category3维度
        SingleOutputStreamOperator<JSONObject> c3DS = AsyncDataStream.unorderedWait(
                withTmInfoDS,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        obj.put("category3_name", dimJsonObj.getString("name"));
                        obj.put("category2_id", dimJsonObj.getString("category2_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("category3_id");
                    }
                }, 60, TimeUnit.SECONDS
        );

        //关联category2维度
        SingleOutputStreamOperator<JSONObject> c2DS = AsyncDataStream.unorderedWait(
                c3DS,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        obj.put("category1_id", dimJsonObj.getString("category1_id"));
                        obj.put("category2_name", dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("category2_id");
                    }
                }, 60, TimeUnit.SECONDS
        );

        //关联category1维度
        SingleOutputStreamOperator<JSONObject> c1DS = AsyncDataStream.unorderedWait(
                c2DS,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        obj.put("category1_name", dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("category1_id");
                    }
                }, 60, TimeUnit.SECONDS
        );
        c1DS.print();
        //写入doris
        c1DS
                .map(jsonObject -> jsonObject.toJSONString(jsonObject))
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));
    }
}








