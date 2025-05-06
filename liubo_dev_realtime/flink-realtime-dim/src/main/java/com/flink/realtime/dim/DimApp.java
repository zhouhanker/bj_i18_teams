package com.flink.realtime.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.struggle.flink.realtime.common.bean.TableProcessDim;
import com.struggle.flink.realtime.common.constant.Constant;
import com.struggle.flink.realtime.common.util.HbaseUtil;
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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.util.*;

/**
 * @ version 1.0
 * @ Package com.flink.realtime.dim.DimApp
 * @ Author liu.bo
 * @ Date 2025/5/3 14:31
 * @ description: DIM-维度层的处理
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO: 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO: 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        //2.6 设置状态后端以及检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck");
        //2.7 设置操作hadoop的用户
        System.setProperty("HAOOP_USER_NAME", "xuang");
        String groupID = "dim_app_group";
        //创建消费者组
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId(groupID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");
        //kafkaStrDS.print();
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector)  {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String type = jsonObj.getString("op");
//                data1 != null && ("flink-realtime".equals(db) && ("c".equals(type) || "u".equals(type) || "d".equals(type) || "r".equals(type)) || data2 != null && data2.length() > 2 && data1.length() > 2
                if ("c".equals(type) || "u".equals(type) || "d".equals(type) || "r".equals(type)) {
                    collector.collect(jsonObj);
                }
            }
        });
//        jsonObjDS.print();

        //Todo 3.使用FlinkCDC读取配置表中的配置信息
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("flink_realtime_dim")
                .tableList("flink_realtime_dim.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
        //mysqlStrDS.print();
        //Todo 4.对配置流中的数据类型进行转换 jsonStr -> 实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    //抛异常 throws Exception
                    public TableProcessDim map(String s)  {
                        //为了处理方便，现将json字符串转换为jsonObj
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            //对配置表进行了一次删除操作，从before属性中获取删除前的配置信息
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            //对配置表进行了读取、添加、修改操作，从after属性中获取最新的配置信息
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

        //Todo 5.根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HbaseUtil.getHbaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HbaseUtil.closeHbaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进行操作的类型
                        String op = tp.getOp();
                        //获取Hbase中维度表的表名
                        String sinkTable = tp.getSinkTable();
                        //获取在HBase中建表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            //丛配置表中删除了一条数据 讲hbase对应的表删掉
                            HbaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取一条数据或添加一条数据，创建表
                            HbaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        } else {
                            //对配置表中修改一套数据，讲hbase的表删除掉，再创建表
                            HbaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            HbaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

        //TODO 6.定义广播流
        //将配置流中的配置信息进行广播--broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 7.将主流业务和广播流配置信息进行关联--connect
        //processElement:处理主流数据
        //processBroadcastElement:处理广播流配置信息
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        SingleOutputStreamOperator<Tuple3<JSONObject, JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple3<JSONObject, JSONObject, TableProcessDim>>() {
                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple3<JSONObject, JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple3<JSONObject, JSONObject, TableProcessDim>> out) throws Exception {
                        //获取处理的数据的表明（也就是业务层/MySQL的数据）
                        String table = jsonObj.getJSONObject("source").getString("table");
                        //获取广播状态
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        TableProcessDim tableProcessDim = null;

                        if ((tableProcessDim = broadcastState.get(table)) != null) {
                            //如果根据表明获取到了对应的配置信息，说明当前处理的是维度数据，将维度表的数据继续到下游传递
                            JSONObject dataJsonObj = jsonObj.getJSONObject("after");
                            JSONObject dataJsonObj2 = jsonObj.getJSONObject("before");

                            //在向下游传递数据前，过滤掉不需要传递的数据
                            String sinkColumns = tableProcessDim.getSinkColumns();
                            if (dataJsonObj != null) {
                                deleteNotNeedColumns(dataJsonObj, sinkColumns);
                            } else if (dataJsonObj2 != null) {
                                deleteNotNeedColumns(dataJsonObj2, sinkColumns);
                            } else {
                                System.err.println("dataJsonObj is null for table: " + table);
                            }
                            String type = jsonObj.getString("op");
                            if (dataJsonObj != null) {
                                dataJsonObj.put("type", type);
                            }
                            if (dataJsonObj2 != null) {
                                dataJsonObj2.put("type", type);
                            }
                            out.collect(Tuple3.of(dataJsonObj2, dataJsonObj, tableProcessDim));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple3<JSONObject, JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple3<JSONObject, JSONObject, TableProcessDim>> out) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取广播状态
                        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String sourceTable = tp.getSourceTable();
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据，将对应的配置信息也从广播流状态中删除
                            broadcastState.remove(sourceTable);
                        } else {
                            //对配置表进行乐都区，还添加或者更新操作，将最新的配置信息放到广播状态中
                            broadcastState.put(sourceTable, tp);
                        }
                    }
                }
        );
        dimDS.print();

        //TODO 8.将维度数据同步到HBase表中

        dimDS.addSink(new RichSinkFunction<Tuple3<JSONObject, JSONObject, TableProcessDim>>() {
            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HbaseUtil.getHbaseConnection();
            }

            @Override
            public void close() throws Exception {
                HbaseUtil.closeHbaseConnection(hbaseConn);
            }

            //将流中数据写入到HBase
            @Override
            public void invoke(Tuple3<JSONObject, JSONObject, TableProcessDim> tup) throws Exception {
                JSONObject jsonObj0 = tup.f0;  // before数据（可能为null）
                JSONObject jsonObj = tup.f1;   // after数据（可能为null）
                TableProcessDim tableProcessDim = tup.f2;

                try {
                    // 处理删除操作（jsonObj0不为null且包含"type"字段）
                    if (jsonObj0 != null && jsonObj0.containsKey("type")) {
                        String type0 = jsonObj0.getString("type");
                        if ("d".equals(type0)) {
                            String sinkTable = tableProcessDim.getSinkTable();
                            String rowKey = jsonObj0.getString(tableProcessDim.getSinkRowKey());
                            HbaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
                            System.out.println("Deleted row with key: " + rowKey);
                        }
                    }

                    // 处理插入/更新操作（jsonObj不为null且包含"type"字段）
                    if (jsonObj != null && jsonObj.containsKey("type")) {
                        String type = jsonObj.getString("type");
                        String sinkTable = tableProcessDim.getSinkTable();
                        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());

                        if ("r".equals(type) || "u".equals(type) || "c".equals(type)) {
                            String sinkFamily = tableProcessDim.getSinkFamily();
                            HbaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObj);
                            System.out.println("Updated row with key: " + rowKey);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Failed to process tuple: " + tup);
                    e.printStackTrace();
                }
//                JSONObject jsonObj0 = tup.f0;
//                JSONObject jsonObj = tup.f1;
//                TableProcessDim tableProcessDim = tup.f2;
//                String type0 = jsonObj0.getString("type");
//                String type = jsonObj.getString("type");
//                jsonObj.remove("type");
//                jsonObj0.remove("type");
//                //获取操作的HBase表的表明
//                String sinkTable = tableProcessDim.getSinkTable();
//                //获取rowkey
//                String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
//
//                //判断对业务数据库维度表进行了什么操作
//                if (jsonObj0.containsKey("type")) {
//                    if ("d".equals(type0)) {
//                        //从业务数据库维度表中做了删除操作 需要将HBase维度表中对应的记录也删除掉
//                        HbaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
//                        System.out.println(type);
//                        System.out.println(jsonObj0);
//                    }
//                }
//                if (jsonObj.containsKey("type")) {
//                    if ("r".equals(type) || "u".equals(type) || "c".equals(type)) {
//                        //如果不是delete，可能的类型有i、u、bootstrap-insert
//                        String sinkFamily = tableProcessDim.getSinkFamily();
//                        HbaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObj);
//                        System.out.println(type);
//                        System.out.println(jsonObj);
//                    }
//                }
            }
        });

        //env.fromSource();
        //dimDS.sinkTo();

        env.execute();
    }

    //
    //过滤掉不需要传递的字段
    //data
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}
