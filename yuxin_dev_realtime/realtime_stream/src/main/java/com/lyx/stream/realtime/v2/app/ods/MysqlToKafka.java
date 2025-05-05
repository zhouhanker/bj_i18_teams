package com.lyx.stream.realtime.v2.app.ods;

import com.lyx.stream.realtime.v2.app.utils.FlinkSinkUtil;
import com.lyx.stream.realtime.v2.app.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.lyx.stream.realtime.v1.app.ods.MysqlToKafka
 * @Author yuxin_li
 * @Date 2025/4/17 9:00
 * @description: MysqlToKafka
 * 基于 Apache Flink 编写
 * 主要功能是将 MySQL 数据库中的数据读取出来
 * 将其发送到 Kafka 主题中
 * 实现了从 MySQL 数据库到 Kafka 消息队列的数据同步功能
 */

public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        //环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //从 MySQL 读取数据
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        mySQLSource.print();

        //创建 Kafka 写入器
        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("yuxin_li_db");

        //将数据写入 Kafka
        mySQLSource.sinkTo(topic_db);


        env.execute("MysqlToKafka");

    }
}



