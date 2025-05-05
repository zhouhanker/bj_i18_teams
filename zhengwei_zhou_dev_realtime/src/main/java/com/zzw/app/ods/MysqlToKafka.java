package com.zzw.app.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zzw.utils.FlinkSinkUtil;
import com.zzw.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.zzw.stream.realtime.v1.app.ods.MysqlToKafka
 * @Author zhengwei_zhou
 * @Date 2025/4/17 9:00
 * @description: MysqlToKafka
 */

public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
// 从Flink的流式执行环境类中获取一个执行环境实例
// 这个执行环境是Flink程序的基础，用于配置和执行流式计算任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 设置任务的并行度为2
// 并行度表示任务在集群中并行执行的线程或进程数量
// 这里设置为2意味着任务将以2个并行的线程或进程来执行
        env.setParallelism(2);

// 调用自定义工具类FlinkSourceUtil的getMySqlSource方法
// 获取一个用于从MySQL数据库中读取数据的数据源对象
// 这里指定要读取的表名为"realtime_v1"，并选择所有列（"*"）
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

// 使用执行环境的fromSource方法，从前面获取的MySQL数据源对象中创建一个数据流
// 同时指定不使用水印（Watermark）策略，因为这里可能不需要处理事件时间
// 并为这个数据源命名为"MySQL Source"，方便后续监控和调试
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        mySQLSource.print();
// 这行代码被注释掉了，如果取消注释，会将从MySQL读取的数据打印到控制台
// 通常用于调试阶段查看数据

// 调用自定义工具类FlinkSinkUtil的getKafkaSink方法
// 获取一个用于将数据写入Kafka主题的Sink对象
// 这里指定要写入的Kafka主题名为"zhengwei_zhou_db"
        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("zhengwei_zhou_db");

// 将从MySQL数据源获取的数据流写入到前面创建的Kafka Sink中
// 即将数据发送到指定的Kafka主题
        mySQLSource.sinkTo(topic_db);

// 调用执行环境的execute方法来启动Flink作业
// 并为这个作业命名为"Print MySQL Snapshot + Binlog"
// 该作业会将从MySQL读取的数据发送到Kafka主题中
        env.execute("Print MySQL Snapshot + Binlog");

    }
}



