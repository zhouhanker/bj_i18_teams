package com.bg.realtime_dws.App;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.bean.TradeSkuOrderBean;
import com.bg.common.function.DimAsyncFunction;
import com.bg.common.util.DateFormatUtil;
import com.bg.common.util.FlinkSinkUtil;
import com.bg.realtime_dws.function.BeanToJsonStrMapFunction;
import com.bg.realtime_dws.function.CustomStringDeserializationSchema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
/**
 * Package package com.bg.realtime_dws.App.DwsTradeSkuOrderWindow
 * Author Chen.Run.ze
 * Date 2025/4/16 14:34
 * description: sku粒度下单业务过程聚合统计
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
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("dwd_trade_order_detail")
                .setGroupId("my-group")
                .setProperty("consumer.ignore.invalid.records", "true") // 跳过无效记录
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CustomStringDeserializationSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                        if (s != null) {
                            collector.collect(JSON.parseObject(s));
                        }
                    }
                }
        );
//        jsonObjDS.print();
        //TODO 2.按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
//        orderDetailIdKeyedDS.print();
        // TODO 去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次接收到的数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //说明重复了 ，将已经发送到下游的数据(状态)，影响到度量值的字段进行取反再传递到下游
                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

//        distinctDS.print();
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<JSONObject>) (jsonObj, recordTimestamp) -> jsonObj.getLong("ts_ms") * 1000
                        )
        );
//        withWatermarkDS.print();
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                (MapFunction<JSONObject, TradeSkuOrderBean>) jsonObj -> {
                    //{"create_time":"2024-06-11 10:54:40","sku_num":"1","activity_rule_id":"5","split_original_amount":"11999.0000",
                    // "split_coupon_amount":"0.0","sku_id":"19","date_id":"2024-06-11","user_id":"2998","province_id":"32",
                    // "activity_id":"4","sku_name":"TCL","id":"15183","order_id":"10788","split_activity_amount":"1199.9",
                    // "split_total_amount":"10799.1","ts":1718160880}
                    String skuId = jsonObj.getString("sku_id");
                    BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                    BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                    BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                    BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                    Long ts = jsonObj.getLong("ts_ms") * 1000;
                    return TradeSkuOrderBean.builder()
                            .skuId(skuId)
                            .originalAmount(splitOriginalAmount)
                            .couponReduceAmount(splitCouponAmount)
                            .activityReduceAmount(splitActivityAmount)
                            .orderAmount(splitTotalAmount)
                            .ts(ts)
                            .build();
                }
        );
//        beanDS.print();

        //TODO 6.分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);
        //TradeSkuOrderBean(stt=null, edt=null, curDate=null, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null, category2Name=null, category3Id=null, category3Name=null, skuId=30, skuName=null, spuId=null, spuName=null, originalAmount=69.0000, activityReduceAmount=0.0, couponReduceAmount=0.0, orderAmount=69.0, ts=1745824056015000)
        //TradeSkuOrderBean(stt=null, edt=null, curDate=null, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null, category2Name=null, category3Id=null, category3Name=null, skuId=17, skuName=null, spuId=null, spuName=null, originalAmount=6699.0000, activityReduceAmount=669.9, couponReduceAmount=0.0, orderAmount=6029.1, ts=1745824055836000)
        //TradeSkuOrderBean(stt=null, edt=null, curDate=null, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null, category2Name=null, category3Id=null, category3Name=null, skuId=35, skuName=null, spuId=null, spuName=null, originalAmount=5499.0000, activityReduceAmount=0.0, couponReduceAmount=0.0, orderAmount=5499.0, ts=1745824056003000)
//        skuIdKeyedDS.print();
        //TODO 7.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 8.聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                (ReduceFunction<TradeSkuOrderBean>) (value1, value2) -> {
                    value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                    value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                    value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                    value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                    return value1;
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) {
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
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
//        reduceDS.print();

        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
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

        //withSkuInfoDS.print();
        //TODO 10.关联spu维度
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
//        withSpuInfoDS.print();
        //TODO 11.关联tm维度
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
        //TODO 12.关联category3维度
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

        //TODO 13.关联category2维度
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

        //TODO 14.关联category1维度
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

        //======>> TradeSkuOrderBean(stt=2025-05-02 16:13:50, edt=2025-05-02 16:14:00, curDate=2025-05-02, trademarkId=2, trademarkName=苹果, category1Id=2, category1Name=手机, category2Id=13, category2Name=手机通讯, category3Id=61, category3Name=手机, skuId=8, skuName=Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机, spuId=3, spuName=Apple iPhone 12, originalAmount=65576.0000, activityReduceAmount=0.0, couponReduceAmount=0.0, orderAmount=65576.0, ts=1745824056383000)
        //======>> TradeSkuOrderBean(stt=2025-05-02 16:13:50, edt=2025-05-02 16:14:00, curDate=2025-05-02, trademarkId=3, trademarkName=联想, category1Id=6, category1Name=电脑办公, category2Id=33, category2Name=电脑整机, category3Id=287, category3Name=游戏本, skuId=13, skuName=联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i9-12900H RTX3060 钛晶灰, spuId=4, spuName=联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑, originalAmount=79192.0000, activityReduceAmount=1500.0, couponReduceAmount=0.0, orderAmount=77692.0, ts=1745824056007000)
        //======>> TradeSkuOrderBean(stt=2025-05-02 16:13:50, edt=2025-05-02 16:14:00, curDate=2025-05-02, trademarkId=9, trademarkName=CAREMiLLE, category1Id=8, category1Name=个护化妆, category2Id=54, category2Name=香水彩妆, category3Id=477, category3Name=唇部, skuId=29, skuName=CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇, spuId=10, spuName=CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏, originalAmount=414.0000, activityReduceAmount=0.0, couponReduceAmount=15.0, orderAmount=399.0, ts=1745824057938000)
        withC1DS.print("======>");

        //TODO 15.将关联的结果写到Doris表中
        withC1DS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));

        env.execute();
    }
}
