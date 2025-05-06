package com.ytx.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import com.ytx.base.BaseApp;
import com.ytx.bean.TableProcessDim;
import com.ytx.constant.Constant;
import com.ytx.realtime.dim.function.TableProcessFunction;
import com.ytx.util.FlinkSourceUtil;
import com.ytx.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;



public class dimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
      new dimApp().start(10001,4,"my_group", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> jsonobjds = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                out.collect(jsonObj);
            }
        });
     jsonobjds.print();
//  配置表读取
        SingleOutputStreamOperator<TableProcessDim> tpds = readTableprocess(env);
//        tpds.print();
        //TODO 根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpds.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
            private Connection hbaseConn;
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn=HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConn(hbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) throws Exception {
                String op = tp.getOp();
                String sinkTable = tp.getSinkTable();
                String[] sinkFamilies = tp.getSinkFamily().split(",");
                if ("d".equals(op)){

                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
                }else if ("r".equals(op)||"c".equals(op)){
                    HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                }else {
                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
                    HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                }
                return tp;
            }
        });

//    tpds.print();
        //将配置流中的配置信息进行广播---broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastds = tpds.broadcast(mapStateDescriptor);
        //将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDs = jsonobjds.connect(broadcastds);
        //处理关联后的数据(判断是否为维度)
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDs = connectDs.process(
                new TableProcessFunction(mapStateDescriptor)

        );
        dimDs.print();
//       dimDs.addSink(new HBaseSinkFunction());

    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableprocess(StreamExecutionEnvironment env) {
        MySqlSource mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1_config", "table_process_dim");

        DataStream<String> mySQLds= env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
      mySQLds.print();
//        配置流数据转换
        SingleOutputStreamOperator<TableProcessDim> tpds = mySQLds.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonstr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonstr);
                String op = jsonObj.getString("op");
                TableProcessDim tableProcessDim = null;
                if ("d".equals(op)) {
                    tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                } else {
                    tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);
        return tpds;
    }
}
