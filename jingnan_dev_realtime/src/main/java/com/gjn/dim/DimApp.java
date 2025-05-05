package com.gjn.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gjn.base.TableProcessDim;
import com.gjn.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Package com.gjn.dim.DimApp
 * @Author jingnan.guo
 * @Date 2025/4/8 10:57
 * @description:
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //开启检查点
        //env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点超时时间
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //设置job取消后是否保留检查点
        //env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置两个检查点最小时间间隔
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //设置重启策咯
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        //设置状态后端及其检查点储存路径
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/2207A/stream_realtime/flink");
        //设置操作hadoop用户
        //System.setProperty("HADOOP_USER_NAME","jingnan_guo");



        //从kafka主题中读取业务数据

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("stream_realtime_dev1")
                .setGroupId("my_group")
                //.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                //在生产环境中，一般为了保证消费的精准一次性，需要手动维护偏移量，KafkaSource->KafkaSourceReader->存储偏移量变量
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 从最末尾位点开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                //注意：如果使用Flink提供的SimpleStringSchema对String类型的消息进行反序列化，如果消息为空，会报错
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null) {
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
      // kafkaDS.print(">>>>kafka输出");
//        //TODO 4.对业务流中数据类型进行转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                String db = jsonObj.getJSONObject("source").getString("db");
                String type = jsonObj.getString("op");
                String data = jsonObj.getString("after");
                if ("stream_realtime".equals(db)
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
        //jsonObjDS.print(">>>>主流");
//
//        //使用flinkCDC读取配置表中的配置信息
        Properties props=new Properties();
        props.setProperty("useSSL","false");
        props.setProperty("allowPublicKeyRetrieval","true");
//
//
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("gmall2024_config")
                .tableList("gmall2024_config.table_process_dim")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
//
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        //mysqlDS.print(">>>>mysql数据表");
//
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonstr) throws Exception {
                        //为了处理方便 现将json string转换为json 对象
                        JSONObject jsonObject = JSON.parseObject(jsonstr);
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if("d".equals(op)){
                            //对配置表进行了一次删除操作  从befor属性中获取删除前的配置信息
                            tableProcessDim  = jsonObject.getObject("befor", TableProcessDim.class);
                        }else{
                            //对配置表进行了读取  添加  修改操作   从 after属性中获取最新的配置信息
                            tableProcessDim  =  jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
//

//
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        hbaseConn=HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        // 获取 hbase 中维度表
                        String sinkTable = tp.getSinkTable();
                        //获取在 hbase 中建表的 列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
                            //从配置表中删除了一条数据  对应的从hbase中删除 该表数据
                            HBaseUtil.dropHBaseTable(hbaseConn,"ns_jingnan_guo",sinkTable);
                        }else if("r".equals(op) || "c".equals(op)){
                            //从配置表中读取了一条数据 或者 添加了一条数据  在hbase建表

                            HBaseUtil.createHBaseTable(hbaseConn,"ns_jingnan_guo",sinkTable,sinkFamilies);
                        }else{
                            //从配置表中进行了修改信息  先从hbase中将对应的表删除 在创建表
                            HBaseUtil.dropHBaseTable(hbaseConn,"ns_jingnan_guo",sinkTable);

                            HBaseUtil.createHBaseTable(hbaseConn,"ns_jingnan_guo",sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        );
        tpDS.print(">>>>hbase输出");
//
        // 将配置流中的配置信息进行广播 -- broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // 将主流业务数据和广播流配置信息进行关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
//
//
        // 处理关联后的数据
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {

                    private Map<String,TableProcessDim> configMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //将配置表中的配置信息预加载到我们的程序configMap中
                        //注册驱动
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        //建立连接
                        java.sql.Connection conn = DriverManager.getConnection("jdbc:mysql://cdh03:3306?useSSL=false", "root", "root");
                        //获取数据库操作对象
                        String sql="select * from gmall2024_config.table_process_dim";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        //执行sql语句
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        //处理结果集
                        while (rs.next()){
                            //定义 一个json 对象 用于接受遍历出来的数据
                            JSONObject jsonObj = new JSONObject();
                            for (int i=1;i<= metaData.getColumnCount();i++){
                                String columnName = metaData.getColumnName(i);
                                Object columnValue = rs.getObject(i);
                                jsonObj.put(columnName,columnValue);
                            }
                            //将jsonObj转换为实体类对象，并放到configmap中
                            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
                            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
                        }
                        //释放资源
                        rs.close();
                        ps.close();
                        conn.close();
                    }

                    // 处理主流业务数据
                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
                        String table = jsonObj.getJSONObject("source").getString("table");
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        //根据表明先到广播状态中获取对应的配置信息  如果没有找到对应的配置 在尝试到 configMap中获取
                        TableProcessDim tableProcessDim = null;


                        if ((tableProcessDim  =  broadcastState.get(table))!=null || (tableProcessDim  =  configMap.get(table))!=null){
                            //如果根据表明获取到了对应的配置信息，说明当前处理的是维度数据    将维度数据继续向下游专递

                            //将维度数据继续向下游传递（只需要传递data属性内容即可）
                            JSONObject dataJsonObj = jsonObj.getJSONObject("after");

                            //在向下游传递之前，过滤掉不需要传递的属性
                            String sinkColumns = tableProcessDim.getSinkColumns();
                            deleteNotNeedColumns(dataJsonObj,sinkColumns);

                            //向下游传递数据前，补充对维度数据的操作类型属性
                            String type = jsonObj.getString("op");
                            dataJsonObj.put("op",type);

                            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
                        }
                    }

                    // 处理广播流中的配置信息
                    @Override
                    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
                        String op = tp.getOp();

                        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        //获取维度表的名称
                        String sourceTable = tp.getSourceTable();
                        if("d".equals(op)){
                            //从配置表中删除了一条配置信息  也从广播状态中进行删除
                            broadcastState.remove(sourceTable);
                            configMap.remove(sourceTable);
                        }else{
                            //对配置信息进行了读取或者添加  更新操作  将配置信息放到广播流状态中
                            broadcastState.put(sourceTable,tp);
                            configMap.put(sourceTable,tp);
                        }
                    }
                }
        );

        dimDS.print(">>>>广播流");

        dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {

            private Connection hbaseConn;
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn=HBaseUtil.getHBaseConnection();

            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseConn);
            }

            //将流中数据写到hbase
            @Override
            public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
                JSONObject jsonObj = tup.f0;
                TableProcessDim tableProcessDim = tup.f1;
                String type = jsonObj.getString("type");
                jsonObj.remove("type");

                //获取操作hbase的表明
                String sinkTable = tableProcessDim.getSinkTable();
                //获取rowkey
                String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
                //判断对我们业务数据库  维度表进行了什么操作
                if("delete".equals(type)){
                    //从业务数据库维度表中做了删除操作      需要将hbase维度表中对应的记录删除掉
                    HBaseUtil.delRow(hbaseConn,"ns_jingnan_guo",sinkTable,rowKey);
                }else{
                    //如果不是delete，可能类型有insert、update、bootstrap-insert，上述操作对应的 都是向hbase表中put数据
                    String sinkFamily = tableProcessDim.getSinkFamily();
                    HBaseUtil.putRow(hbaseConn,"ns_jingnan_guo",sinkTable,rowKey,sinkFamily,jsonObj);
                }
            }
        });

        env.execute();
    }
    //过滤掉不需要传递的字段
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        JSONObject newJsonObj = new JSONObject();
        for(String column:columnArr){
            newJsonObj.put(column,dataJsonObj.getString(column));
        }
    }

}
