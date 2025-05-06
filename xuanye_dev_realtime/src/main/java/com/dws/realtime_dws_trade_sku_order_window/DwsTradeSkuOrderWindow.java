package com.dws.realtime_dws_trade_sku_order_window;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.Bean.TradeSkuOrderBean;
import com.common.base.BaseApp;
import com.common.constant.Constant;
import com.common.function.DimAsyncFunction;
import com.common.utils.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * @author Felix
 * @date 2024/6/12
 * sku粒度下单业务过程聚合统计
 *      维度：sku
 *      度量：原始金额、优惠券减免金额、活动减免金额、实付金额
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、redis、doris、DwdTradeOrderDetail、DwsTradeSkuOrderWindow
 * 开发流程
 *      基本环境准备
 *      检查点相关的设置
 *      从kafka的下单事实表中读取数据
 *      空消息的处理并将流中数据类型进行转换  jsonStr->jsonObj
 *      去重
 *          为什么会产生重复数据？
 *              我们是从下单事实表中读取数据的，下单事实表由订单表、订单明细表、订单明细活动表、订单明细优惠券表四张表组成
 *              订单明细是主表，和订单表进行关联的时候，使用的是内连接
 *              和订单活动以及订单明细优惠券表进行关联的时候，使用的是左外连接
 *              如果左外连接，左表数据先到，右表数据数据后到，查询结果会由3条数据
 *                  左表      null    标记为+I
 *                  左表      null    标记为-D
 *                  左表      右表     标记为+I
 *              这样的数据，发送到kafka主题，kafka主题会接收到3条消息
 *                  左表  null
 *                  null
 *                  左表  右表
 *              所以我们在从下单事实表中读取数据的时候，需要过滤空消息，并去重
 *          去重前：需要按照唯一键进行分组
 *          去重方案1：状态+定时器
 *              当第一条数据到来的时候，将数据放到状态中保存起来，并注册5s后执行的定时器
 *              当第二条数据到来的时候，会用第二条数据的聚合时间和第一条数据的聚合时间进行比较，将时间大的数据放到状态中
 *              当定时器被触发执行的时候，将状态中的数据发送到下游
 *              优点：如果出现重复了，只会向下游发送一条数据，数据不会膨胀
 *              缺点：时效性差
 *          去重方案2：状态+抵消
 *              当第一条数据到来的时候，将数据放到状态中，并向下游传递
 *              当第二条数据到来的时候，将状态中影响到度量值的字段进行取反，传递到下游
 *              并将第二条数据也向下游传递
 *              优点：时效性好
 *              缺点：如果出现重复了，向下游传递3条数据，数据出现膨胀
 *      指定Watermark以及提取事件时间字段
 *      再次对流中数据进行类型转换   jsonObj->实体类对象 （相当于wordcount封装二元组的过程）
 *      按照统计的维度进行分组
 *      开窗
 *      聚合计算
 *      维度关联
 *          最基本的实现      HBaseUtil->getRow
 *          优化1：旁路缓存
 *              思路：先从缓存中获取维度数据，如果从缓存中获取到了维度数据(缓存命中)，直接将其作为结果进行返回；
 *                  如果在缓存中，没有找到要关联的维度，发送请求到HBase中进行查询，并将查询的结果放到缓存中缓存起来，方便下次查询使用
 *              选型：
 *                  状态      性能很好，维护性差
 *                  redis    性能不错，维护性好      √
 *              关于Redis的设置
 *                  key：    维度表名:主键值
 *                  type：   String
 *                  expire:  1day   避免冷数据常驻内存，给内存带来压力
 *                  注意：如果维度数据发生了变化，需要将清除缓存      DimSinkFunction->invoke
 *          优化2：异步IO
 *              为什么使用异步？
 *                  在flink程序中，想要提升某个算子的处理能力，可以提升这个算子的并行度，但是更多的并行度意味着需要更多的硬件资源，不可能无限制
 *                  的提升，在资源有限的情况下，可以考虑使用异步
 *              异步使用场景：
 *                  用外部系统的数据，扩展流中数据的时候
 *              默认情况下，如果使用map算子，对流中数据进行处理，底层使用的是同步的处理方式，处理完一个元素后再处理下一个元素，性能较低
 *              所以在做维度关联的时候，可以使用Flink提供的发送异步请求的API，进行异步处理
 *              AsyncDataStream.[un]orderedWait(
 *                  流,
 *                  如何发送异步请求，需要实现AsyncFunction接口,
 *                  超时时间,
 *                  时间单位
 *              )
 *              HBaseUtil添加异步读取数据的方法
 *              RedisUtil添加异步读写数据的方法
 *              封装了一个模板类，专门发送异步请求进行维度关联
 *                  class DimAsyncFunction extends RichAsyncFunction[asynvInvoke] implements DimJoinFunction[getRowKey、getTableName、addDims]{
 *                      asyncInvoke:
 *                          //创建异步编排对象，有返回值
 *                          CompletableFuture.supplyAsync
 *                          //执行线程任务  有入参、有返回值
 *                          .thenApplyAsync
 *                          //执行线程任务  有入参、无返回值
 *                          .thenAcceptAsync
 *                  }
 *      将数据写到Doris中
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,
                "source12"
        );

    }
    @Override
    public void Handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> jsonobjDs = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                if (jsonStr != null) {
                    out.collect(JSON.parseObject(jsonStr));
                }
            }
        });
//        jsonobjDs.print();
//按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> keyedDs = jsonobjDs.keyBy(jsonObject -> jsonObject.getString("id"));
        //去重方式2：状态 + 抵消    优点：时效性好    缺点：如果出现重复，需要向下游传递3条数据(数据膨胀)
        SingleOutputStreamOperator<JSONObject> distinctDs = keyedDs.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastJsonObjState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor =
                        new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastJsonObj = lastJsonObjState.value();
                if (lastJsonObj != null) {
                    String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                    String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                    String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                    String splitTotalAmount = lastJsonObj.getString("split_total_amount");



                    lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                    lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                    lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                    lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                    out.collect(jsonObj);
                }
                lastJsonObjState.update(jsonObj);
                out.collect(jsonObj);
            }
        });
//        distinctDs.print();
//        指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDs.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms");
                                    }
                                }
                        )
        );
//        对流中数据进行类型转换
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDs = withWatermarkDS.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                String skuId = jsonObj.getString("sku_id");
                BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                long ts = jsonObj.getLong("ts_ms");
                TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                        .skuId(skuId)
                        .originalAmount(splitOriginalAmount)
                        .couponReduceAmount(splitCouponAmount)
                        .activityReduceAmount(splitActivityAmount)
                        .orderAmount(splitTotalAmount)
                        .ts(ts)
                        .build();
                return orderBean;
            }
        });
//      beanDs.print();

//    分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDs = beanDs.keyBy(TradeSkuOrderBean::getSkuId);
//        开窗
        AllWindowedStream<TradeSkuOrderBean, TimeWindow> windowDS  =
                skuIdKeyedDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1)));
//        聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDs = windowDS.reduce(new ReduceFunction<TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;
            }
            },
                new ProcessAllWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, TimeWindow>.Context ctx, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = iterable.iterator().next();
                        TimeWindow window = ctx.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);

                    }
                }
        );
//   reduceDs.print();
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDs,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }
                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//       withSkuInfoDS.print();
//        关联spu维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//       withSpuInfoDS.print();
        // 11.关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // 12.关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //13.关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // 14.关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withC1DS = AsyncDataStream.unorderedWait(
                c2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        withC1DS.print();
//        withC1DS.map(new BeanToJsonStrMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));
    }



}
