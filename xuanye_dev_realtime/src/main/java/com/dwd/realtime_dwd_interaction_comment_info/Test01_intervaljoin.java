package com.dwd.realtime_dwd_interaction_comment_info;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Test01_intervaljoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从指定的网络端口读取员工数据
        SingleOutputStreamOperator<Emp> empDs = env
                .socketTextStream("cdh02", 8888)
                .map(
                        new MapFunction<String, Emp>() {
                            @Override
                            public Emp map(String lineStr) throws Exception {
                                String[] fieldArr = lineStr.split(",");
                                return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                            }
                        }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Emp>() {
                                            @Override
                                            public long extractTimestamp(Emp emp, long l) {
                                                return emp.getTs();
                                            }
                                        }
                                )
                );

        //从指定的网络端口

        SingleOutputStreamOperator<Dept> deptDs = env
                .socketTextStream("cdh02", 8889)
                .map(
                        new MapFunction<String, Dept>() {
                            @Override
                            public Dept map(String lineStr) throws Exception {
                                String[] fieldArr = lineStr.split(",");
                                return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1], Long.valueOf(fieldArr[2]));
                            }
                        }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Dept>() {
                                            @Override
                                            public long extractTimestamp(Dept dept, long l) {
                                                return dept.getTs();
                                            }
                                        }
                                )
                );
        //怎么样使用interjoin关联
        empDs
                .keyBy(Emp::getDeptno)
                .intervalJoin(deptDs.keyBy(Dept::getDeptno))
                .between(Time.milliseconds(-5),Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<Emp, Dept, Tuple2<Emp,Dept>>() {
                            @Override
                            public void processElement(Emp emp, Dept dept, ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>.Context context, Collector<Tuple2<Emp, Dept>> collector) throws Exception {
                                collector.collect(Tuple2.of(emp,dept));
                            }
                        }
                ).print();

        env.execute();
    }
}
