package com.rb.ods;

import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.rb.ods.CdcSinkToKafka
 * @Author runbo.zhang
 * @Date 2025/4/10 9:42
 * @description:
 */
public class CdcSinkToKafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> allData = SourceSinkUtils.cdcRead(env, "online_flink_retail", "*");
        DataStreamSource<String> processData = SourceSinkUtils.cdcRead(env, "online_flink_retail_process", "table_process_dim");
        allData.print("allData");
        processData.print("processData");
        allData.sinkTo(SourceSinkUtils.sinkToKafka("log_topic_flink_online_v1"));
        processData.sinkTo(SourceSinkUtils.sinkToKafka("log_topic_flink_online_process_v1"));


        env.execute("aaaaaaaa");

    }
}
