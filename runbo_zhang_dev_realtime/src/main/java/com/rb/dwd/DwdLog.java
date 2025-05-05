package com.rb.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.DateFormatUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package com.rb.dwd.DwdTradeOrderDetail
 * @Author runbo.zhang
 * @Date 2025/4/10 13:53
 * @description:
 */
public class DwdLog {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        DataStreamSource<String> kafkaRaed = SourceSinkUtils.kafkaRead(env, "log_topic_flink_online_v1_log");

        OutputTag<String> notJsonTag = new OutputTag<String>("notJsonTag") {
        };
        SingleOutputStreamOperator<String> filtedDataStream = filterNotJson(kafkaRaed, notJsonTag);
        SideOutputDataStream<String> notJsonData = filtedDataStream.getSideOutput(notJsonTag);

        SingleOutputStreamOperator<JSONObject> fixNewOldDt = fixNewOld(filtedDataStream);

        fixNewOldDt.print("fffffffffffff");

        fixNewOldDt.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.sinkToKafka("log_topic_flink_online_v2_log"));
//
        notJsonData.print("nnnnnnnnn");
        notJsonData.sinkTo(SourceSinkUtils.sinkToKafka("log_topic_flink_online_v2_log_not_json"));

        OutputTag<String> pageTag = new OutputTag<String>("page") {
        };
        SingleOutputStreamOperator<String> process = fixNewOldDt.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                if (value.containsKey("page")) {
                    ctx.output(pageTag, value.toJSONString());
                }
            }
        });
        SideOutputDataStream<String> pageDate = process.getSideOutput(pageTag);

        pageDate.sinkTo(SourceSinkUtils.sinkToKafka("log_topic_flink_online_v2_log_page"));

        env.disableOperatorChaining();
        env.execute();

    }
    public static SingleOutputStreamOperator<String> filterNotJson(DataStreamSource<String> dataStream,OutputTag<String> outputTag){


        SingleOutputStreamOperator<String> process = dataStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                try {
                    JSONObject object = JSON.parseObject(value);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
                out.collect(value);
            }
        });
        return process;

    }






    public static  SingleOutputStreamOperator<JSONObject> fixNewOld(SingleOutputStreamOperator<String> dataStream){
        SingleOutputStreamOperator<JSONObject> fixNewOldDt = dataStream.map(JSON::parseObject).keyBy(o -> o.getJSONObject("common").getString("mid")).map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDate", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

                    }

                    @Override
                    public JSONObject map(JSONObject o) throws Exception {
                        String isNew = o.getJSONObject("common").getString("is_new");
                        Long ts = o.getLong("ts");
                        String visitDate = DateFormatUtil.tsToDate(ts);
                        String lastDate = lastVisitDateState.value();

                        //  isnew=1
                        if ("1".equals(isNew)) {
                            //上次访问日期是1
                            if (StringUtils.isEmpty(lastDate)) {
                                //状态是空则说明没有状态 则加入状态
                                lastVisitDateState.update(visitDate);
                            } else {

                                if (!lastDate.equals(visitDate)) {
                                    isNew = "0";
                                    o.getJSONObject("common").put("is_new", isNew);
                                    System.out.println("已修正");
                                    System.out.println(o.toJSONString());
                                }
                            }

                        } else {
                            //  isnew=0
                            if (StringUtils.isEmpty(lastDate)) {
                                String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterday);
                            }

                        }


                        return o;
                    }
                }
        );
        return fixNewOldDt;
    }
}
