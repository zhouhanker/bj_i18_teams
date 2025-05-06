package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.fuction.DimAsync;
import com.rb.utils.DateFormatUtil;
import com.rb.utils.HbaseUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.rb.dws.DwsSkuOrder
 * @Author runbo.zhang
 * @Date 2025/4/16 15:28
 * @description:
 */
public class DwsSkuOrder {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/DwsSkuOrder1");
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "dwd_trade_order_detail_v1");
//        kafkaRead.print();
        SingleOutputStreamOperator<JSONObject> process = kafkaRead.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {


                try {
                    out.collect(JSON.parseObject(value));
                } catch (Exception e) {
                    System.out.println("有脏数据");
                }


            }
        });
        KeyedStream<JSONObject, String> keyedStream = process.keyBy(o -> o.getString("id"));
//        kafkaRead.print();
        SingleOutputStreamOperator<JSONObject> distinctDs = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> stateDescriptor = new ValueStateDescriptor<>("dsfsdf", JSONObject.class);
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                valueState = getRuntimeContext().getState(stateDescriptor);
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


        SingleOutputStreamOperator<JSONObject> wateredDs = distinctDs.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
        WindowedStream<JSONObject, String, TimeWindow> windowDs = wateredDs.keyBy(o -> o.getString("sku_id"))
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)));
        SingleOutputStreamOperator<JSONObject> reduce =
                windowDs
                        .reduce(new ReduceFunction<JSONObject>() {
                                    @Override
                                    public JSONObject reduce(JSONObject v1, JSONObject v2) throws Exception {
                                        v1.put("reduce_original_amount", v1.getDouble("split_original_amount") + v2.getDouble("split_original_amount"));
                                        v1.put("reduce_coupon_amount", v1.getDouble("split_coupon_amount") + v2.getDouble("split_coupon_amount"));
                                        v1.put("reduce_activity_amount", v1.getDouble("split_activity_amount") + v2.getDouble("split_activity_amount"));
                                        v1.put("reduce_total_amount", v1.getDouble("split_total_amount") + v2.getDouble("split_total_amount"));
                                        return v1;
                                    }
                                }, new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                                    @Override
                                    public void process(String s, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
                                        JSONObject next = elements.iterator().next();
                                        TimeWindow window = context.window();
                                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                                        next.put("stt", stt);
                                        next.put("edt", edt);
                                        next.put("cur_date", curDate);
                                        out.collect(next);

                                    }
                                }
                        );
//        reduce.print("lkj");

        //todo 异步连接sku_id
        DataStream<JSONObject> resultStream =
                AsyncDataStream.unorderedWait(reduce,
                        //如何发送异步请求
                        new RichAsyncFunction<JSONObject, JSONObject>() {
                            private AsyncConnection hbaseCon;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseCon = HbaseUtil.getHbaseAsyncCon();
                            }

                            @Override
                            public void close() throws Exception {
                                HbaseUtil.closeHbaseAsyncCon(hbaseCon);
                            }

                            @Override
                            public void asyncInvoke(JSONObject data, ResultFuture<JSONObject> resultFuture) throws Exception {
                                String skuId = data.getString("sku_id");
                                JSONObject dimAsync = HbaseUtil.readDimAsync(hbaseCon, "dim_zrb_online_v1", "dim_sku_info", skuId);
                                if (dimAsync != null) {
                                    data.put("sku_name", dimAsync.getString("sku_name"));
                                    data.put("spu_id", dimAsync.getString("spu_id"));
                                    data.put("category3_id", dimAsync.getString("category3_id"));
                                    data.put("tm_id", dimAsync.getString("tm_id"));
                                    //处理后的数据传入下游
                                    resultFuture.complete(Collections.singleton(data));

                                } else {
                                    System.out.println("未查询到dim维度数据，关联失败");
//                                    System.out.println("aaaaaa"+skuId);
//                                    System.out.println("aaaaaaa"+dimAsync.toJSONString());
                                }

                            }
                        },
                        200, TimeUnit.SECONDS);
//        resultStream.print();

        //todo 异步连接spu_id
        SingleOutputStreamOperator<JSONObject> spu_connect = AsyncDataStream.unorderedWait(resultStream,
                new DimAsync<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        String spuName = dimJsonObj.getString("spu_name");
                        obj.put("spu_name", spuName);
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("spu_id");
                    }
                },
                100, TimeUnit.SECONDS);
//spu_connect.print();
        // todo 连接c3
        SingleOutputStreamOperator<JSONObject> c3_connect = AsyncDataStream.unorderedWait(spu_connect, new DimAsync<JSONObject>() {
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
        }, 60, TimeUnit.SECONDS);
//        c3_connect.print();

        //连接c2
        SingleOutputStreamOperator<JSONObject> c2_connect = AsyncDataStream.unorderedWait(c3_connect, new DimAsync<JSONObject>() {
            @Override
            public void addDims(JSONObject obj, JSONObject dimJsonObj) {

                obj.put("category2_name", dimJsonObj.getString("name"));
                obj.put("category1_id", dimJsonObj.getString("category1_id"));
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public String getRowKey(JSONObject obj) {
                return obj.getString("category2_id");
            }
        }, 60, TimeUnit.SECONDS);


        //连接c1
        SingleOutputStreamOperator<JSONObject> c1_connect = AsyncDataStream.unorderedWait(c2_connect, new DimAsync<JSONObject>() {
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
        }, 60, TimeUnit.SECONDS);

//连接c1
        SingleOutputStreamOperator<JSONObject> tm_connect = AsyncDataStream.unorderedWait(c1_connect, new DimAsync<JSONObject>() {
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
        }, 60, TimeUnit.SECONDS);

        tm_connect.print();
        tm_connect.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.getDorisSink("doris_database_v1", "dws_trade_sku_order_window"));
        env.disableOperatorChaining();
        env.execute();
    }

}
