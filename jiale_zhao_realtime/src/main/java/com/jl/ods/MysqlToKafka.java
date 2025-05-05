package com.jl.ods;



import com.jl.utils.FlinkSinkUtil;
import com.jl.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Package com.jl.MysqlToKafka
 * @Author jia.le
 * @Date 2025/4/9 16:25、.
 * @description: MysqlToKafka
 */

public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        mySQLSource.print();

        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("topic_db");

        mySQLSource.sinkTo(topic_db);

        env.execute("Print MySQL Snapshot + Binlog");

    }
}
