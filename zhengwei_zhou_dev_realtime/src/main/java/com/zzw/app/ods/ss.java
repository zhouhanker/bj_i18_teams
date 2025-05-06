package com.zzw.app.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zzw.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.zzw.stream.realtime.v1.app.ods.ss
 * @Author zhengwei_zhou
 * @Date 2025/4/22 13:35
 * @description: ss
 */
public class ss {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(),"MySQL Source");
        mySQLSource.print();


    }
}
