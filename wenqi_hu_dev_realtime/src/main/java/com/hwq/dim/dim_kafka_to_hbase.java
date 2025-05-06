package com.hwq.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.common.bean.TableProcessDin;
import com.hwq.common.until.HbaseUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;

import java.util.Properties;

/**
 * @Package com.zsf.retail.v1.realtime.dim.kafka_to_hbase
 * @Author hu.wen.qi
 * @Date 2025/5/4
 * @description:
 */
public class dim_kafka_to_hbase {
    public static void main(String[] args) throws Exception {
        // 环境准备：流处理，
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
        // 检查点相关设置：
        // 开启检查点，
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 超时时间，
        env.getCheckpointConfig().setCheckpointTimeout(6000000L);
        // job取消后检查是否保留点，
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 两个检查点之间的最小时间间隔，
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(20000L);
        // 重启策略，
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        // 设置状态后端，// 检查点存储路径，
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/2207A/");
        // 设置操作hadoop用户
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        // 从kafka读取数据：声明消费者组，创建消费者对象，消费数据，封装为流
        String groupId = "dim_app_group";
        // 创建消费者对象，
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setTopics("log_topic")
                .setGroupId(groupId)
                //在生成环境中，一般为了保证消费的精准一次，需要手动维护偏移量，kafkaSource-->kafkaSourceReader-->存储偏移量
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // 从末尾点开始读取
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 如果使用flink提供的SimpleStringSchema对String类型的消息反序列化，如果为空，会报错
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 对业务流中数据类型进行转换 jsonstr -->  jsonObject
        DataStreamSource<String> dbStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        // 使用flink cdc：创建mysqlsourc对象，封装为流

        SingleOutputStreamOperator<JSONObject> dbObjDS = dbStrDS.map(JSON::parseObject);
        // dbObjDS.print();

//        dbObjDS.print("dbobj-->");
        // 对配置流中数据类型进行转换 jsonstr -->  jsonObject
        // 根据配置表中的配置信息到hbase 中执行建表或者删除表操作
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "double");
        properties.setProperty("time.precision.mode", "connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("dev_realtime_v7_wenqi_hu") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("dev_realtime_v7_wenqi_hu.table_process_dim") // 设置捕获的表
                .username("root")
                .password("root")
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.earliest()) //全量
//                .startupOptions(StartupOptions.latest()) //增量
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStreamSource<String> dbDimStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        //dbDimStrDS.print("sx_002_v2--->");

        //把jsonobject转换成实体类
        SingleOutputStreamOperator<TableProcessDin> dbDimTabDS = dbDimStrDS.map(new MapFunction<String, TableProcessDin>() {
            @Override
            public TableProcessDin map(String s) {
                JSONObject jsonObj = JSONObject.parseObject(s);
                String op = jsonObj.getString("op");
                TableProcessDin after = null;
                if ("d".equals(op)) {
                    //如果是删除获取删除前数据before
                    after = jsonObj.getObject("before", TableProcessDin.class);
                } else {
                    //非删除获取更新后的数据after
                    after = jsonObj.getObject("after", TableProcessDin.class);
                }

                after.setOp(op);
                return after;
            }
        });

        //调用方法 把维度表信息写入到 hbase
        // createHBaseTable(dbDimTabDS);


        // 将配置流中的配置信息进行广播 -----broadcast

        MapStateDescriptor<String, TableProcessDin> mapStateDescriptor = new MapStateDescriptor<>("state", String.class, TableProcessDin.class);
        //设置广播流
        BroadcastStream<TableProcessDin> broadcast = dbDimTabDS.broadcast(mapStateDescriptor);
        //主流connect广播流
        BroadcastConnectedStream<JSONObject, TableProcessDin> connect = dbObjDS.connect(broadcast);
        //侧流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {
        };

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDin>> dimDS = connect.process(new BroadcastProcessFunction<JSONObject, TableProcessDin, Tuple2<JSONObject, TableProcessDin>>() {
            //主流
            @Override
            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDin, Tuple2<JSONObject, TableProcessDin>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDin>> out) throws Exception {
                //主流 状态变量
                ReadOnlyBroadcastState<String, TableProcessDin> state = readOnlyContext.getBroadcastState(mapStateDescriptor);

                Thread.sleep(100);
                System.out.println("processElement" + jsonObj);
                //获取主流内的table表名
                String table = jsonObj.getJSONObject("source").getString("table");

                //根据表名先到广播状态中获取对应的配置信息，如果没有找到对应的配置，再尝试到configMap中获取
                TableProcessDin TableProcessDin = null;

                if ((TableProcessDin = state.get(table)) != null) {
                    //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据

                    // 将维度数据继续向下游传递(只需要传递data属性内容即可)
                    JSONObject dataJsonObj = jsonObj.getJSONObject("after");

                    //在向下游传递数据前，过滤掉不需要传递的属性
                    String sinkColumns = TableProcessDin.getSinkColunns();
//                    deleteNotNeedColumns(dataJsonObj, sinkColumns);

                    //在向下游传递数据前，补充对维度数据的操作类型属性
                    String op = jsonObj.getString("op");
                    dataJsonObj.put("type", op);

                    out.collect(Tuple2.of(dataJsonObj, TableProcessDin));
                } else {
                    readOnlyContext.output(hbaseTag, jsonObj);
                }
            }

            //广播流
            @Override
            public void processBroadcastElement(TableProcessDin tp, BroadcastProcessFunction<JSONObject, TableProcessDin, Tuple2<JSONObject, TableProcessDin>>.Context context, Collector<Tuple2<JSONObject, TableProcessDin>> collector) throws Exception {
                //广播流
                System.out.println("processBroadcastElement" + tp);
                //获取 op 操作状态
                String op = tp.getOp();
                //获取 要写入状态算子的 数据
                String sourceTable = tp.getSourceTable();
                //获取 状态算子
                BroadcastState<String, TableProcessDin> state = context.getBroadcastState(mapStateDescriptor);
                //判断 是否是删除操作
                if ("d".equals(op)) {
                    //删除 状态算子 里对应的key的数据
                    state.remove(sourceTable);
                } else {
                    //非删除 状态算子 里添加对应的数据
                    state.put(sourceTable, tp);
                }
            }
        });
        dimDS.print("1-->");


        dimDS.addSink(new SinkFunction<Tuple2<JSONObject, TableProcessDin>>() {
            @Override
            public void invoke(Tuple2<JSONObject, TableProcessDin> value, Context context) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDin TableProcessDin = value.f1;
                String type = jsonObj.getString("type");
                jsonObj.remove("type");

                //获取操作的HBase表的表名
                String sinkTable = TableProcessDin.getSinkTable();
                //获取rowkey
                String rowKey = jsonObj.getString(TableProcessDin.getSinkRowKey());
                //判断对业务数据库维度表进行了什么操作
                HbaseUtils hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");
                if ("d".equals(type)) {
                    //从业务数据库维度表中做了删除操作  需要将HBase维度表中对应的记录也删除掉
                   // HbaseUtils.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
                } else {
                    //如果不是delete，可能的类型有insert、update、bootstrap-insert，上述操作对应的都是向HBase表中put数据
                    String sinkFamily = TableProcessDin.getSinkFamily();
                   // HbaseUtils.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObj);
                    // 2. 配置要写入的表名、行键和数据
                     // 替换为实际的表名
                    TableName tableName = TableName.valueOf("dim_to_hbases", sinkTable);
                    System.out.println(tableName);
                    // 3. 创建 BufferedMutator，用于批量写入数据
                    BufferedMutatorParams params = new BufferedMutatorParams(tableName);
                    BufferedMutator mutator = hbaseUtils.getConnection().getBufferedMutator(params);

                    // 4. 调用 HbaseUtils 的 put 方法写入数据
                    HbaseUtils.put(rowKey, jsonObj, mutator);

                    // 5. 刷新缓冲区并关闭 BufferedMutator
                    mutator.flush();
                    mutator.close();

                    // 6. 关闭 HBase 连接（可选，根据实际情况决定是否关闭）
                    hbaseUtils.getConnection().close();

                    System.out.println("数据已成功写入 HBase 表：" + tableName);
                }
            }
        });
        env.execute();


//    private static SingleOutputStreamOperator<TableProcessDin> createHBaseTable
//            (SingleOutputStreamOperator<TableProcessDin> tpDS) {
//        tpDS = tpDS.map(new MapFunction<TableProcessDin, TableProcessDin>() {
//            @Override
//            public TableProcessDin map(TableProcessDin tp) throws Exception {
//                HbaseUtils hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");
//
//                if (hbaseUtils.isConnect()) {
//                    System.out.println("HBase 连接正常");
//                } else {
//                    System.out.println("HBase 连接失败");
//                }
//
//                //获取配置表：操作类型
//                String op = tp.getOp();
//
//                //获取配置表：hbase维度表表名
//                String sinkTable = tp.getSinkTable();
//                //获取配置表：hbase维度表表中列祖
//                String[] sinkFamily = tp.getSinkFamily().split(",");
//
//                if ("d".equals(op)) {
//                    //从配置表：删除一条数据，hbase将对应的表删除
//                    boolean tableDeleted = hbaseUtils.deleteTable(sinkTable);
//
//                } else if ("r".equals(op) || "c".equals(op)) {
//                    //从配置表：添加一条数据，hbase将对应的表添加
//                    boolean tableCreated = hbaseUtils.createTable(Constant.HBASE_NAMESPACE, sinkTable, sinkFamily);
//
//                } else {
//                    //从配置表：修改一条数据，hbase将对应的表修改：先删除后添加
//                    boolean tableCreated = hbaseUtils.createTable(Constant.HBASE_NAMESPACE, sinkTable, sinkFamily);
//                    boolean tableDeleted = hbaseUtils.deleteTable(sinkTable);
//
//                }
//                return tp;
//            }
//        }).setParallelism(1);
//        return tpDS;
//    }


    }
}

