package com.zsf.retail_v1.realtime.ods;

import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zsf.retail_v1.realtime.util.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.zsf.retail_v1.realtime.ods.dbMysql2Kafka
 * @Author zhao.shuai.fei
 * @Date 2025/4/23 16:43
 * @description:
 */
public class dbMysql2Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","double");
        properties.setProperty("time.precision.mode","connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("sx_002") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("sx_002.*") // 设置捕获的表
                .username("root")
                .password("root")
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.earliest()) //全量
//                .startupOptions(StartupOptions.latest()) //增量
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置 3s 的 checkpoint 间隔
        //env.enableCheckpointing(3000);

        DataStreamSource<String> ds1 = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//        ds1.print();


        SingleOutputStreamOperator<String> ds2 = ds1.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                boolean b = JSON.isValid(s);
                if (b == false) {
                    return false;
                }
                if (JSON.parseObject(s).getString("after") == null) {
                    return false;
                }
                return true;
            }
        });
        ds2.print();

        ds2.addSink(KafkaUtil.getKafkaSink("topic_db"));

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
