package com.zgq.stream.realtime.v2.app.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zgq.utils.FlinkSinkUtil;
import com.zgq.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.zgq.stream.realtime.v2.app.ods.MysqlToKafka
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:55
 * @description:
 */

public class MysqlToKafka {
 public static void main(String[] args) throws Exception {
  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  env.setParallelism(1);

  MySqlSource <String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

  DataStreamSource <String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        mySQLSource.print();

  KafkaSink <String> topic_db = FlinkSinkUtil.getKafkaSink("topic_db");

  mySQLSource.sinkTo(topic_db);

  env.execute("Print MySQL Snapshot + Binlog");

 }
}