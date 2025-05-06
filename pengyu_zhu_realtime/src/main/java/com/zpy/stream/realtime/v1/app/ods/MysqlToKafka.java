package com.zpy.app.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zpy.utils.FlinkSinkUtil;
import com.zpy.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * MySQL 到 Kafka 的数据传输程序
 * 功能：使用 Flink CDC 捕获 MySQL 数据库变更，并将数据实时同步到 Kafka
 * @Package com.zpy.app.ods.MysqlToKafka
 * @Author pengyu_zhu
 */
public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置作业并行度为2
        env.setParallelism(2);

        // 2. 创建 MySQL 数据源
        // 使用工具类获取 MySQL CDC 源，监控 realtime_v1 数据库的所有表
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

        // 3. 从 MySQL 源创建数据流
        // 不使用水位线策略（因为这里是数据同步，不需要时间相关的计算）
        DataStreamSource<String> mySQLSource = env.fromSource(
                realtimeV1,
                WatermarkStrategy.noWatermarks(),
                "MySQL Source"  // 算子名称
        );


        // 4. 创建 Kafka Sink
        // 使用工具类获取 Kafka Sink，目标主题为 pengyu_zhu_db
        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("pengyu_zhu_db");

        // 5. 将 MySQL 数据流写入 Kafka
        mySQLSource.sinkTo(topic_db);

        // 6. 执行作业
        env.execute("Print MySQL Snapshot + Binlog");
    }
}