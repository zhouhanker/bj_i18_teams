package com.slh.app.ods;


import com.slh.utils.FlinkSinkUtil;
import com.slh.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.slh.app.ods.Mysql_To_Kafka
 * @Author lihao_song
 * @Date 2025/4/23 15:30
 * @description:
 */
public class Mysql_To_Kafka {
    public static void main(String[] args) throws Exception {
        //创建流模式
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //检查点
        env.setParallelism(2);
        //mysql中的数据库的名字
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("dev_realtime_v1", "*");
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "Mysql Source");

        mySQLSource.print();

        KafkaSink<String> topicDb = FlinkSinkUtil.getKafkaSink("topic_db");

        mySQLSource.sinkTo(topicDb);

        env.execute();
    }
}
