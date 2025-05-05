package com.hwq.dwd.new_old_xf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.common.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package com.hwq.dwd.dwd
 * @Author hu.wen.qi
 * @Date 2025/4/10 14:13
 * @description: 1
 */

public class log_to_kafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "topic_log", "ac");
        //kafkaSource.print();


        //分流
        OutputTag<String> startTag = new OutputTag<String>("side-start") {};
        OutputTag<String> pageTag = new OutputTag<String>("side-page") {};
        OutputTag<String> displaysTag = new OutputTag<String>("side-displays") {};
        OutputTag<String> actionsTag = new OutputTag<String>("side-actions") {};
        OutputTag<String> errTag = new OutputTag<String>("side-err") {};

        SingleOutputStreamOperator<String> rizhi_liu = kafkaSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) {
                JSONObject all = JSON.parseObject(s);
                if (s.contains("start")) {
                    context.output(startTag, s);
                }
                if (s.contains("page")) {
                    context.output(pageTag, s);
                }
                if (s.contains("displays")) {
                    context.output(displaysTag, s);
                }
                if (s.contains("actions")) {

                    context.output(actionsTag, s);
                }
                if (s.contains("err")) {
                    context.output(errTag, s);
                }
            }
        });

        SideOutputDataStream<String> start_Ds = rizhi_liu.getSideOutput(startTag);
        start_Ds.print("start_Ds");
        SideOutputDataStream<String> page_Ds = rizhi_liu.getSideOutput(pageTag);
        page_Ds.print("page_Ds");
        SideOutputDataStream<String> displays_Ds = rizhi_liu.getSideOutput(displaysTag);
        displays_Ds.print("displays_Ds");
        SideOutputDataStream<String> actions_Ds = rizhi_liu.getSideOutput(actionsTag);
        actions_Ds.print("actions_Ds");
        SideOutputDataStream<String> err_Ds = rizhi_liu.getSideOutput(errTag);
        err_Ds.print("err_Ds");

//        start_Ds.addSink(KafkaUtil.getKafkaSink("start_log"));
//        page_Ds.addSink(KafkaUtil.getKafkaSink("page_log"));
//        displays_Ds.addSink(KafkaUtil.getKafkaSink("displays_log"));
//        actions_Ds.addSink(KafkaUtil.getKafkaSink("actions_log"));
//        err_Ds.addSink(KafkaUtil.getKafkaSink("err_log"));



        env.execute();
    }
}
