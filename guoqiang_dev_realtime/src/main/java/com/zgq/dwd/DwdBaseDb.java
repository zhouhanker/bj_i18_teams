package com.zgq.stream.realtime.v2.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zgq.bean.TableProcessDwd;
import com.zgq.constant.Constant;
import com.zgq.function.BaseDbTableProcessFunction;
import com.zgq.utils.FlinkSinkUtil;
import com.zgq.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.zgq.stream.realtime.v2.app.dwd.DwdBaseDb
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:37
 * @description:
 */

public class DwdBaseDb {
    public static void main(String[] args) throws Exception {

//        环境设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

//        从 Kafka 读取数据
        KafkaSource <String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dwd_log");
        DataStreamSource <String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

//        kafkaStrDS.print();

//          处理 Kafka 数据
        SingleOutputStreamOperator < JSONObject > jsonObjDS = kafkaStrDS.process(
                new ProcessFunction <String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector <JSONObject> out) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            throw new RuntimeException("不是一个标准的json");
                        }

                    }
                }
        );
//        jsonObjDS.print();


//        从 MySQL 读取数据
        MySqlSource <String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v2","table_process_dwd");
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");

//        处理 MySQL 数据
        SingleOutputStreamOperator< TableProcessDwd > tpDS = mysqlStrDS.map(
                new MapFunction <String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr) {

                        JSONObject jsonObj = JSON.parseObject(jsonStr);

                        String op = jsonObj.getString("op");
                        TableProcessDwd tp = null;
                        if("d".equals(op)){
                            tp = jsonObj.getObject("before", TableProcessDwd.class);
                        }else{
                            tp = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                }
        );
//        tpDS.print();


//        广播配置信息
        MapStateDescriptor <String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<>("mapStateDescriptor",String.class, TableProcessDwd.class);
        BroadcastStream <TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);

//        连接数据流并处理
        BroadcastConnectedStream <JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
        SingleOutputStreamOperator< Tuple2 <JSONObject, TableProcessDwd> > splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));


//        输出结果并执行作业
        splitDS.print();
        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());

        env.execute("DwdBaseDb");

    }
}
