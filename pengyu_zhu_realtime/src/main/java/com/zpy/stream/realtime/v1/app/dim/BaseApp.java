package com.zpy.app.dim;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zpy.bean.TableProcessDim;
import com.zpy.constant.Constant;
import com.zpy.function.HBaseSinkFunction;
import com.zpy.function.TableProcessFunction;
import com.zpy.utils.FlinkSourceUtil;
import com.zpy.utils.HBaseUtil;
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
 * 实时维度数据处理主程序
 * 功能：从Kafka读取业务数据，从MySQL读取配置信息，处理后写入HBase
 * @Package com.zpy.app.bim.BaseApp
 * @Author pengyu_zhu
 */
public class BaseApp {
    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为4
        env.setParallelism(4);

        // 启用检查点，每5秒一次，保证精确一次语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 2. 从Kafka读取数据源
        // 使用工具类获取Kafka源，主题为Constant.TOPIC_DB，消费者组为dim_app
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");

        // 创建Kafka数据流，不设置水位线
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 3. 处理Kafka数据流
        // 过滤出符合条件的数据：数据库为realtime_v1，操作为增删改查，且after字段有效
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        // 解析JSON字符串
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 获取源数据库信息
                        String db = jsonObj.getJSONObject("source").getString("db");
                        // 获取操作类型
                        String type = jsonObj.getString("op");
                        // 获取变更后的数据
                        String data = jsonObj.getString("after");

                        // 过滤条件：
                        // 1. 数据库为realtime_v1
                        // 2. 操作为创建(c)、更新(u)、删除(d)或读取(r)
                        // 3. after字段不为空且长度大于2
                        if ("realtime_v1".equals(db)
                                && ("c".equals(type) || "u".equals(type) || "d".equals(type) || "r".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);  // 输出符合条件的JSON对象
                        }
                    }
                }
        );

        // 4. 从MySQL读取配置信息
        // 使用工具类获取MySQL源，读取realtime_v1_config.table_process_dim表
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1_config", "table_process_dim");

        // 创建MySQL数据流，并行度设为1
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

        // 5. 处理MySQL配置数据流
        // 将JSON字符串转换为TableProcessDim对象，并根据操作类型处理数据
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;

                        // 如果是删除操作，取before数据
                        if("d".equals(op)){
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else{
                            // 其他操作取after数据
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        // 设置操作类型
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

        // 6. 动态管理HBase表结构
        // 使用RichMapFunction管理HBase连接，并根据配置动态创建/删除HBase表
        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;  // HBase连接

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化HBase连接
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        // 关闭HBase连接
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");

                        // 根据操作类型处理HBase表
                        if("d".equals(op)){
                            // 删除操作：删除HBase表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
                            // 读取或创建操作：创建HBase表
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }else{
                            // 其他操作（如更新）：先删除再创建表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

        // 7. 广播连接处理
        // 创建广播状态描述符，用于存储维度表配置信息
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);

        // 将配置流广播出去
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // 连接Kafka数据流和广播配置流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        // 使用自定义函数处理连接后的流，将业务数据与配置信息配对
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS
                .process(new TableProcessFunction(mapStateDescriptor));

        // 8. 数据写入HBase
        // 使用自定义的HBaseSinkFunction将处理后的数据写入HBase
        dimDS.addSink(new HBaseSinkFunction());

        // 9. 执行作业
        env.execute("dim");
    }
}