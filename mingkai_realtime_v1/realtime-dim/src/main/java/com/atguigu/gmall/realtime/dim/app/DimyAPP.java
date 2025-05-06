package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.time.Time;
import org.apache.hadoop.hbase.client.Connection;


import java.sql.*;
import java.util.*;

public class DimyAPP {
    public static void main(String[] args) throws Exception {
        //TODO 1. 基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2. 检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

        //2.6 设置状态后端以及检查点存储路径
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/zmkzmk");

//        System.setProperty("HADOOP_USER_NAME", "hdfs");

        String topic = "topic_db";
        String groupid = "zmkzmk";
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupid)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
//        kafkaStrDS.print();

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String db = jsonObj.getString("database");
                String type = jsonObj.getString("type");
                String data = jsonObj.getString("data");

                if ("gmall2024".equals(db)
                        && ("insert".equals(type)
                        || "update".equals(type)
                        || "delete".equals(type)
                        || "boostrap-insert".equals(type))
                        && data != null
                        && data.length() > 2
                ) {
                    out.collect(jsonObj);
                }
            }
        });

//        jsonObjDs.print();
//        //mysql监控数据
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        // 创建MySQLSource对象
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST) // 替换为实际MySQL主机名
                .port(Constant.MYSQL_PORT) // 替换为实际MySQL端口
                .databaseList("gmall2024_config") // 替换为实际数据库名
                .tableList("gmall2024_config.*") // 替换为实际表名
                .username(Constant.MYSQL_USER_NAME) // 替换为实际用户名
                .password(Constant.MYSQL_PASSWORD) // 替换为实际密码
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();

        DataStreamSource<String> mysqlStrDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
//        mysqlStrDS.print();
//////
////        //TT6.对配置流中的数据类型进行转换   jsonStr -> 实体类
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
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

//        tpDS.print();
////////
////////        //根据配置表中的配置信息到Hbase中执行建表或者删除表等操作
        tpDS = tpDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception{
                    hbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                    HBaseUtil.closeHBaseConnection(hbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) throws Exception {

                String op = tp.getOp();

                String sinkTable = tp.getSinkTable();

                String[] sinkFamilies = tp.getSinkFamily().split(",");

                if ("d".equals(op)){
                    HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                }else if ("r".equals(op)||"c".equals(op)){
                    HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE, sinkTable,sinkFamilies);
                }else {
                    HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                    HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE, sinkTable,sinkFamilies);
                }
                return tp;
            }
        }).setParallelism(1);

//        tpDS.print();
////////
////////        T8.将配置流中的配置信息进行广播--broadcast
//        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
//                new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
//        BroadcastStream<TableProcessDim> broadcasDs = tpDS.broadcast(mapStateDescriptor);
//
//        //T9.将主流业务数据和广播流配置信息进行关联--connect
//        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDs = jsonObjDs.connect(broadcasDs);
//
//        //T10.处理关联后的数据(判断是否为维度)
//        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDs.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {
//
//            private Map<String, TableProcessDim> configMap = new HashMap<>();
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//
//                Class.forName("com.mysql.jdbc.Driver");
//
//                java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
//
//                String sql = "select * from gmall2024_config.table_process_dim";
//
//                PreparedStatement ps = conn.prepareStatement(sql);
//
//                ResultSet rs = ps.executeQuery();
//                ResultSetMetaData metaData = rs.getMetaData();
//
//                while (rs.next()) {
//                    JSONObject jsonObj = new JSONObject();
//                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
//                        String columnName = metaData.getColumnName(i);
//                        Object columnValue = rs.getObject(i);
//                        jsonObj.put(columnName, columnValue);
//                    }
//                    TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
//                    configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
//                }
//
//                rs.close();
//
//                ps.close();
//
//                conn.close();
//            }
//
//            @Override
//            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
//                String table = jsonObj.getString("table");
//                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//
//                TableProcessDim tableProcessDim = null;
//
//                if ((tableProcessDim = broadcastState.get(table)) != null
//                        || (tableProcessDim = configMap.get(table)) != null) {
//                    // 如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据
//
//                    // 将维度数据继续向下游传递(只需要传递data属性内容即可)
//                    JSONObject dataJsonObj = jsonObj.getJSONObject("data");
//
//                    // 在向下游传递数据前，过滤掉不需要传递的属性
//                    String sinkColumns = tableProcessDim.getSinkColumns();
//                    deleteNotNeedColumns(dataJsonObj, sinkColumns);
//
//                    // 在向下游传递数据前，补充对维度数据的操作类型属性
//                    String type = jsonObj.getString("type");
//                    dataJsonObj.put("type", type);
//
//                    out.collect(Tuple2.of(dataJsonObj, tableProcessDim));
//                }
//            }
//
//            @Override
//            public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
//                String op = tp.getOp();
//                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//
//                String sourceTable = tp.getSourceTable();
//                if("d".equals(op)){
//                    broadcastState.remove(sourceTable);
//                    configMap.remove(sourceTable);
//                }else {
//                    broadcastState.put(sourceTable,tp);
//                    configMap.put(sourceTable,tp);
//                }
//            }
//        });
//
//        dimDS.print();
////////
////
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDs.connect(broadcastDS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS  = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {
                    private Map<String,TableProcessDim>configMap= new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //注册驱动
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        //建立连接
                        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
                        //获取数据库操作对象
                        //获取数据库操作对象
                        String sql = "select * from gmall2024_config.table_process_dim";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        //执行SQL语句
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        //处理结果集
                        while (rs.next()) {
                            //定义一个json对象，用于接收遍历出来的数据
                            JSONObject json0bj = new JSONObject();
                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                String columnName = metaData.getColumnName(i);
                                Object columnValue = rs.getObject(i);
                                json0bj.put(columnName, columnValue);
                                //将json0bi转换为实体类对象，并放到configMap
                                TableProcessDim tableProcessDim = json0bj.toJavaObject(TableProcessDim.class);
                                configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                                //释放资源rs.close();ps.close();conn.close();
                            }

                        }
                        rs.close();
                        ps.close();
                        conn.close();
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
                        String table = jsonObj.getString("table");
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        TableProcessDim  tableProcessDim=null;
                        if((tableProcessDim=broadcastState.get(table))!=null
                                ||(tableProcessDim=configMap.get(table))!=null){
                            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                            String sinkColumns = tableProcessDim.getSinkColumns();
                            deleteNotNeedColumns(dataJsonObj,sinkColumns);
                            String type = jsonObj.getString("type");
                            dataJsonObj.put("type",type);
                            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
                        }
                    }


                    @Override
                    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
                        String op = tp.getOp();
                        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String sourceTable = tp.getSourceTable();
                        if ("d".equals(op)){
                            broadcastState.remove(sourceTable);
                            configMap.remove(sourceTable);
                        }else {
                            broadcastState.put(sourceTable,tp);
                            configMap.put(sourceTable,tp);
                        }
                    }
                });

        dimDS.print();
//////
//        dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
//            private Connection hbaseConn;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                hbaseConn = HBaseUtil.getHBaseConnection();
//            }
//
//            @Override
//            public void close() throws Exception {
//                HBaseUtil.closeHBaseConnection(hbaseConn);
//            }
//
//            //将流中数据写出到HBase
//            @Override
//            public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
//                JSONObject jsonObj = tup.f0;
//                TableProcessDim tableProcessDim = tup.f1;
//                String type = jsonObj.getString("type");
//                jsonObj.remove("type");
//
//                String sinkTable = tableProcessDim.getSinkTable();
//
//                String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
//
//                //判断对业务数据库维度表进行了什么操作
//                if ("delete".equals(type)) {
//                    //从业务数据库维度表中做了删除操作，需要将HBase维度表中对应的记录也删除掉
//                    HBaseUtil.delRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey);
//                } else {
//                    String sinkFamily = tableProcessDim.getSinkFamily();
//                    HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
//                }
//            }
//        });

//        //TODO 5.提交作业
        env.execute();
    }
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}

