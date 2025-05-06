package com.bhz.app.dim;
/**
 * @Package com.bhz.app.dim.BaseApp
 * @Author huizhong.bai
 * @Date 2025/5/2 16:06
 * @description:
 */


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bhz.bean.TableProcessDim;
import com.bhz.constant.Constant;
import com.bhz.function.TableProcessFunction;
import com.bhz.util.FlinkSourceUtil;
import com.bhz.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;


/**
 * @Package com.bhz.app.dim.BaseApp
 * @Author huizhong.bai
 * @Date 2025/5/2 16:06
 * @description:
 */
public class BaseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        kafkaStrDS.print();

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
                                                                                @Override
                                                                                public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                                                                                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                                                                                    String db = jsonObj.getJSONObject("source").getString("db");
                                                                                    String type = jsonObj.getString("op");
                                                                                    String data = jsonObj.getString("after");

                                                                                    if ("realtime_v1".equals(db)
                                                                                            && ("c".equals(type)
                                                                                            || "u".equals(type)
                                                                                            || "d".equals(type)
                                                                                            || "r".equals(type))
                                                                                            && data != null
                                                                                            && data.length() > 2
                                                                                    ) {
                                                                                        out.collect(jsonObj);
                                                                                    }
                                                                                }
                                                                            }
        );

        jsonObjDs.print();


        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v2", "table_process_dim");


        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);


        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDim tableProcessDim;
                if ("d".equals(op)) {
                    tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                } else {
                    tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);

//        tpDS.print();

        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

//        tpDS.print();

        //定义广播状态
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor",String.class, TableProcessDim.class);
        //把配置数据转成广播流
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //把json数据和广播流连接
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDs.connect(broadcastDS);

        //通过Process方法
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS
                .process(new TableProcessFunction(mapStateDescriptor));

        dimDS.print();

//        dimDS.addSink(new HBaseSinkFunction());

        env.execute("dim");
    }
}
