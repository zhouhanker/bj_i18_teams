package com.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.Bean.TableProcessDim;
import com.common.base.BaseApp;
import com.common.utils.Constat;
import com.common.utils.FlinkSourceUtil;
import com.common.utils.HbaseUtils1;
import com.retailersv1.function.HBaseSinkFunction;
import com.retailersv1.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;


public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001,1,"aaa", Constat.TOPIC_DB,"source_yewu");
    }


    @Override
    public void Handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceDS) {
        //TODO 对业务流中数据数据类型进行ETL jsonstr->jsonobj
        SingleOutputStreamOperator<JSONObject> jsonObjDs = etl(kafkaSourceDS);
//        jsonObjDs.print();
        //TODO 使用FlinkCDC读取配置表中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);
        tpDS.print();
       //TODO 根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpDS = createHBaseTable(tpDS);
//        表空间xuanye_chang下的表dim_coupon_info已存在
        //TODO 过滤维度数据
//        //TODO 8.将配置流中的配置信息进行广播---broadcast
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDs, tpDS);
        //TODO  11.将维度表数据传到Hbase中去
        //({"tm_name":"苹果15","op":"u","id":2},TableProcessDim(sourceTable=base_trademark, sinkTable=dim_base_trademark, sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=c))
        dimDS.print();
        writetoHBase(dimDS);
    }

    private void writetoHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction()).setParallelism(1);
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDs, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        //将配置流进行广播--broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor("maps",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDs = tpDS.broadcast(mapStateDescriptor);

        //TODO 9.将主流业务数据和广播流配置信息进行关联--connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDs.connect(broadcastDs);

        //TODO 10处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
                            private Connection hbaseConn;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseConn = HbaseUtils1.getHBaseConnection();
                            }
                            @Override
                            public void close() throws Exception {
                                HbaseUtils1.closeHBaseConnection(hbaseConn);
                            }
                            @Override
                            public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                                //获取对配置表进行操作的类型
                                String op = tableProcessDim.getOp();
                                String sinkTable = tableProcessDim.getSinkTable();
                                //获取hbase中表的列组
                                String[] sinkFamilies = tableProcessDim.getSinkFamily().split(",");
                                if ("d".equals(op)){
                                    //从配置表中删除一条数据   将HBase中对应的表删除
                                    HbaseUtils1.dropHBaseTable(hbaseConn, Constat.HBASE_NAMESPACE,sinkTable);
                                }else if("r".equals(op)||"c".equals(op)){
                                    //从配置表中读取了一条数据或者添加一条数据   在HBase中执行建表
                                    HbaseUtils1.createHBaseTable(hbaseConn,Constat.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                                }else{
                                    HbaseUtils1.dropHBaseTable(hbaseConn, Constat.HBASE_NAMESPACE,sinkTable);
                                    HbaseUtils1.createHBaseTable(hbaseConn,Constat.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                                }
                                return tableProcessDim;
                            }
                        }
        ).setParallelism(1);
        return tpDS;

    }

    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1", "table_process_dim");
        DataStreamSource<String> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4

                .setParallelism(1);// 设置 sink 节点并行度为 1
//        kafkaSource_log.print();
        //5.1创建MySQLSource对象
        //5.2读取数据封装为流
//       ds.print();

        //TODO 6.对配置流中的数据类型进行转换 jsonStr->实体类对象

        SingleOutputStreamOperator<TableProcessDim> tpDS = ds.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String op = jsonObject.getString("op");
                TableProcessDim commonTable = null;
                if ("d".equals(op)) {
                    commonTable = jsonObject.getObject("before", TableProcessDim.class);
                } else {
                    commonTable = jsonObject.getObject("after", TableProcessDim.class);
                }
                commonTable.setOp(op);
                return commonTable;
            }
        }).setParallelism(1);

        tpDS.print();

        return tpDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSourceDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSourceDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        String db = jsonObj.getJSONObject("source").getString("db");
                        String type = jsonObj.getString("op");
                        String data = jsonObj.getString("ts_ms");
                        if ("realtime_v1".equals(db)
                                && ("c".equals(type)
                                || "u".equals(type)
                                || "d".equals(type)
                                || "r".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            collector.collect(jsonObj);
                        }
                    }
                });
        return jsonObjDs;
    }
}
