package com.lyx.stream.realtime.v2.app.bim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lyx.stream.realtime.v2.app.bean.TableProcessDim;
import com.lyx.stream.realtime.v2.app.constant.Constant;
import com.lyx.stream.realtime.v2.app.function.HBaseSinkFunction;
import com.lyx.stream.realtime.v2.app.function.TableProcessFunction;
import com.lyx.stream.realtime.v2.app.utils.FlinkSourceUtil;
import com.lyx.stream.realtime.v2.app.utils.HBaseUtil;
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
 * @Package com.lyx.stream.realtime.v2.app.bim.BaseApp
 * @Author yuxin_li
 * @Date 2025/4/11 9:34
 * @description: BaseApp
 * 基于 Apache Flink 编写的流式数据处理程序
 * 主要功能是从 Kafka 和 MySQL 中读取数据对数据进行处理和过滤
 * 根据 MySQL 中的配置信息在 HBase 中创建或删除表最后将处理后的数据写入 HBase
 */
public class BaseApp {
    public static void main(String[] args) throws Exception {
        //创建一个 StreamExecutionEnvironment 对象用于配置和执行流式计算任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为 4
        env.setParallelism(4);
        //启用检查点机制，检查点间隔为 5000 毫秒（即 5 秒）
        env.enableCheckpointing(5000L , CheckpointingMode.EXACTLY_ONCE);

        //从 Kafka 读取数据
        //使用 FlinkSourceUtil 工具类获取 Kafka 数据源
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");
        //通过 env.fromSource 方法将 Kafka 数据源添加到 Flink 环境中
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //kafkaStrDS.print();

        //处理 Kafka 数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                //使用 ProcessFunction 对 Kafka 读取的 JSON 字符串进行处理
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {

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

//        jsonObjDS.print();

        //从 MySQL 读取数据
        //使用 FlinkSourceUtil 工具类获取 MySQL 数据源
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1_config", "table_process_dim");

        //通过 env.fromSource 方法将 MySQL 数据源添加到 Flink 环境中
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

//        mysqlStrDS.print();

        // 处理 MySQL 数据
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if("d".equals(op)){
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else{
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

//        tpDS.print();

        //根据 MySQL 数据操作 HBase 表
        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;
                    //在 open 方法中获取 HBase 连接
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    //在 close 方法中关闭 HBase 连接
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

//         tpDS.print();

        //广播 MySQL 数据
        //将 tpDS 数据流广播到所有并行任务中，创建一个 BroadcastStream 对象
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);


        //连接 Kafka 数据和广播数据
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        // 处理连接后的数据
        //使用自定义的 TableProcessFunction 对连接后的数据流进行处理
//        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS
//                .process(new TableProcessFunction(mapStateDescriptor));
//
////        dimDS.print();
//
//        //将处理后的数据写入 HBase
//        dimDS.addSink(new HBaseSinkFunction());

        env.execute("dim");

    }
}
