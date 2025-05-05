package com.rb.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.SourceSinkUtils;
import com.rb.utils.HbaseUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.rb.dim.DimToHbase
 * @Author runbo.zhang
 * @Date 2025/4/8 18:59
 * @description: 1
 */
public class DimToHbase {
    @SneakyThrows
    public static void main(String[] args)  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> dbData = SourceSinkUtils.cdcRead(env, "online_flink_retail", "*");
// todo       DataStreamSource<String> processData = SourceSinkUtils.cdcRead(env, "online_flink_retail_process", "table_process_dim");
        DataStreamSource<String> dbData = SourceSinkUtils.kafkaRead(env, "log_topic_flink_online_v1");
        DataStreamSource<String> processData = SourceSinkUtils.kafkaRead(env, "log_topic_flink_online_process_v1");
//        dbData.print("dbData");
//        processData.print("processData");
        //广播流
        MapStateDescriptor<String, String> dimRuleStateDescriptor = new MapStateDescriptor<>(
                "online_flink_retail_process",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {
                }));
        //声明为广播流
        BroadcastStream<String> broadcastStream = processData.broadcast(dimRuleStateDescriptor);
        //cdc流连接广播流
        KeyedStream<String, String> allData = dbData.keyBy(v -> "aa");
        BroadcastConnectedStream<String, String> connectedStream = allData.connect(broadcastStream);

        OutputTag<String> dwdOutputTag = new OutputTag<String>("dwd_table") {
        };
        //通过广播流过滤维度表
//        SingleOutputStreamOperator<Tuple2<String, String>> dimTablesStream =
        SingleOutputStreamOperator<Tuple2<String, String>> dimTablesStream = connectedStream
                .process(new KeyedBroadcastProcessFunction<String, String, String, Tuple2<String, String>>() {
                    //KeyedBroadcastProcessFunction
                    @Override
                    public void processElement(String value, KeyedBroadcastProcessFunction<String, String, String, Tuple2<String, String>>.ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        JSONObject object = JSON.parseObject(value);
                        String table = object.getJSONObject("source").getString("table");
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(dimRuleStateDescriptor);

                        String process = broadcastState.get(table);
                        String after = object.getString("after");
                        //广播变量中没有数据则延迟五秒处理

                        if (!broadcastState.immutableEntries().iterator().hasNext()) {
                            long l = ctx.timerService().currentProcessingTime() + 5000;
                            ctx.timerService().registerProcessingTimeTimer(l);
                        } else {
                            if (process != null) {//获取到配置信息则是维度表
                                out.collect(Tuple2.of(after, process));
                            } else {
                                ctx.output(dwdOutputTag, value);
                            }
                            ctx.timerService().deleteProcessingTimeTimer(ctx.timerService().currentProcessingTime());
                        }


                    }

                    @Override
                    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, String, String, Tuple2<String, String>>.OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {

                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(dimRuleStateDescriptor);
                        if (!broadcastState.immutableEntries().iterator().hasNext()) {
                            //如果还是空则继续延迟5s
                            long l = ctx.timerService().currentProcessingTime() + 5000;
                            ctx.timerService().registerProcessingTimeTimer(l);
                        }
                    }

                    @Override
                    public void processBroadcastElement(String value, KeyedBroadcastProcessFunction<String, String, String, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        JSONObject dim_process = JSON.parseObject(value);
                        String op = dim_process.getString("op");

                        String table = dim_process.getJSONObject("after").getString("source_table");
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(dimRuleStateDescriptor);

                        if ("d".equals(op)) {//如果是删除则删除这个表
                            broadcastState.remove(table);
                        } else {
                            //如果不是则添加

                            broadcastState.put(table, value);
                        }

                    }
                });
        SideOutputDataStream<String> sideOutput = dimTablesStream.getSideOutput(dwdOutputTag);

        //zhuliu> ({"birthday":4937,"create_time":1744240721000,"login_name":"3hd4p2a","nick_name":"春春","name":"尹春","user_level":"1","phone_num":"13447522216","id":150,"email":"3hd4p2a@0355.net"}
        // {"before":null,"after":{"source_table":"user_info","sink_table":"dim_user_info","sink_family":"info","sink_columns":"id,login_name,name,user_level,birthday,gender,create_time,operate_time","sink_row_key":"id"},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"online_flink_retail_process","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744196070945,"transaction":null})
        dimTablesStream.print("zhuliu");
        //celiu> {"before":null,"after":{"id":5251,"log":"{\"common\":{\"ar\":\"28\",\"ba\":\"OPPO\",\"ch\":\"web\",\"is_new\":\"0\",\"md\":\"OPPO Remo8\",\"mid\":\"mid_302\",\"os\":\"Android 13.0\",\"sid\":\"76ce22b1-057e-49e3-a55d-fcd792d91721\",\"uid\":\"66\",\"vc\":\"v2.1.134\"},\"page\":{\"during_time\":17628,\"last_page_id\":\"mine\",\"page_id\":\"order_list\"},\"ts\":1744210322533}"},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"online_flink_retail","sequence":null,"table":"z_log","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744188374568,"transaction":null}
        sideOutput.print("celiu");




        dimTablesStream.addSink(hbaseSink());
        sideOutput.sinkTo(SourceSinkUtils.sinkToKafka("log_topic_flink_online_v1_dwd"));


        env.disableOperatorChaining();
        env.execute("Print MySQL Snapshot + Binlog111111");
    }
    public static SinkFunction<Tuple2<String, String>> hbaseSink(){
        RichSinkFunction<Tuple2<String, String>> sinkFunction = new RichSinkFunction<Tuple2<String, String>>() {
            private Connection hbaseConnection;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConnection = HbaseUtil.getHbaseConnection();
            }

            @Override
            public void close() throws Exception {
                HbaseUtil.closeHbaseConnection(hbaseConnection);
            }

            @Override
            public void invoke(Tuple2<String, String> tp) throws Exception {
                String data = tp.f0;
                JSONObject dataJson = JSON.parseObject(data);
                String process = tp.f1;
                JSONObject processJson = JSON.parseObject(process);
                String op = processJson.getString("op");
                String sinkTable = processJson.getJSONObject("after").getString("sink_table");
                String sinkRowKey = dataJson.getString(processJson.getJSONObject("after").getString("sink_row_key"));
                if ("d".equals(op)) {//删除操作

                    HbaseUtil.delRow(hbaseConnection, "dim_zrb_online_v1", sinkTable, sinkRowKey);

                } else {//不是删除操作
                    String family = processJson.getJSONObject("after").getString("sink_family");
                    HbaseUtil.putRow(hbaseConnection, "dim_zrb_online_v1", sinkTable, sinkRowKey, family, dataJson.toJSONString());
                }
            }
        };
        return sinkFunction;
    }
}
