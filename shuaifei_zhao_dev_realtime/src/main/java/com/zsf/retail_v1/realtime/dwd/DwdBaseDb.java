package com.zsf.retail_v1.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import com.zsf.retail_v1.realtime.util.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Package com.zsf.retail_v1.realtime.dwd.DwdBaseDb
 * @Author zhao.shuai.fei
 * @Date 2025/4/23 22:34
 * @description:
 */
public class DwdBaseDb {
    @SneakyThrows
    public static void main(String[] args) {
        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
//3.2     创建消费者对象
        DataStreamSource<String> dbStrDS = KafkaUtil.getKafkaSource(env, "topic_db", "group03");

        SingleOutputStreamOperator<JSONObject> dbObjDS1 = dbStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                boolean b = JSONObject.isValid(s);
                if (b){
                    JSONObject jsonObj = JSON.parseObject(s);
                    out.collect(jsonObj);
                }
            }
        });
        SingleOutputStreamOperator<JSONObject> dbObjDS = dbObjDS1.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String string = jsonObject.getJSONObject("source").getString("table");
                if ("user_info".equals(string)) {
                    return true;
                }
                return false;
            }
        });

        //读取配置表信息
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","double");
        properties.setProperty("time.precision.mode","connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("sx_002_v2") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("sx_002_v2.table_process_dwd") // 设置捕获的表
                .username("root")
                .password("root")
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.earliest()) //全量
//                .startupOptions(StartupOptions.latest()) //增量
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStreamSource<String> dbDimStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        SingleOutputStreamOperator<JSONObject> tpDS = dbDimStrDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        //"op":"r": {"before":null,"after":{"source_table":"activity_info","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1716812196180,"transaction":null}
                        //"op":"c": {"before":null,"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"aa"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812267000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11423611,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1716812265698,"transaction":null}
                        //"op":"u": {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"aa"},"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"aa"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812311000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11423960,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1716812310215,"transaction":null}
                        //"op":"d": {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"aa"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812341000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11424323,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1716812340475,"transaction":null}

                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取操作类型
                        String op = jsonObj.getString("op");
                        JSONObject tp = null;
                        if("d".equals(op)){
                            //对配置表进行了删除操作   需要从before属性中获取删除前配置信息
                            tp = jsonObj.getObject("before", JSONObject.class);
                        }else{
                            //对配置表进行了读取、插入、更新操作   需要从after属性中获取配置信息
                            tp = jsonObj.getObject("after", JSONObject.class);
                        }
                        tp.put("op",op);
                        return tp;
                    }
                }
        );
//        tpDS.print();

        //TODO 对配置流进行广播 ---broadcast
        MapStateDescriptor<String, JSONObject> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, JSONObject.class);

        BroadcastStream<JSONObject> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        //TODO 关联主流业务数据和广播流中的配置数据   --- connect
        BroadcastConnectedStream<JSONObject, JSONObject> connectDS = dbObjDS.connect(broadcastDS);

        SingleOutputStreamOperator<Tuple2<JSONObject, JSONObject>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));

        splitDS.print("1-->");

        splitDS.map(Tuple2::toString).addSink(KafkaUtil.getKafkaSink("topicDb2hbaseDim"));
        env.execute();
    }
}
