package com.ytx.realtime.dwd.basedb.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import com.ytx.base.BaseApp;
import com.ytx.bean.TableProcessDwd;
import com.ytx.constant.Constant;
import com.ytx.realtime.dwd.basedb.function.BaseDbTableProcessFunction;
import com.ytx.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

/*

 */
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        // 对流中的数据进行类型转换并进行简单的ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonobj = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    out.collect(jsonObj);
                } catch (Exception e) {
                    throw new RuntimeException("不是一个标准的json");
                }
            }
        });
//        jsonobj.print();
//        使用flinkcdc读取配置表中的配置信息
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1_config", "table_process_dwd");
//      读取数据封装为流
        DataStreamSource<String> mysqlStrDs = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
//      对流中数据进行类型装换
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDs.map(new MapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDwd tp = null;
                if ("d".equals(op)) {
                    tp = jsonObj.getObject("before", TableProcessDwd.class);
                } else {
                    tp = jsonObj.getObject("after", TableProcessDwd.class);
                }
                tp.setOp(op);
                return tp;
            }
        });
//       tpDS.print();
//        对配置流进行广播
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor",
                String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);
//        关联业务数据和广播流中的配置数据
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonobj.connect(broadcastDS);
//        对关联后的数据进行处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> process = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
//        将处理逻辑比较简单的事实表数据写到kafka的不同主题中
        process.print();
//       process.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
