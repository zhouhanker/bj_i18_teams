package com.cj.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.bean.CommonTable;
import com.cj.constant.Constant;
import com.cj.dim.flinkfcation.Tablepeocessfcation;
import com.cj.dim.flinkfcation.flinksinkHbase;
import com.cj.utils.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Package com.cj.dim.dim_app
 * @Author chen.jian
 * @Date 2025/5/5 下午6:35
 * @description:
 */
public class dim_app {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("topic_db")
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

        Properties props=new Properties();
        props.setProperty("useSSL","false");
        props.setProperty("allowPublicKeyRetrieval","true");
//
//
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("gmall2025_config")
                .tableList("gmall2025_config.table_process_dim")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();

        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");

        SingleOutputStreamOperator<CommonTable> tpDS  = mysqlDS.map(
                new MapFunction<String, CommonTable>() {
                    @Override
                    public CommonTable map(String jsonstr) throws Exception {
                        //为了处理方便 现将json string转换为json 对象
                        JSONObject jsonObject = JSON.parseObject(jsonstr);
                        String op = jsonObject.getString("op");
                        CommonTable tableProcessDim = null;
                        if("d".equals(op)){
                            //对配置表进行了一次删除操作  从befor属性中获取删除前的配置信息
                            tableProcessDim  = jsonObject.getObject("befor", CommonTable.class);
                        }else{
                            //对配置表进行了读取  添加  修改操作   从 after属性中获取最新的配置信息
                            tableProcessDim  =  jsonObject.getObject("after", CommonTable.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        );

        tpDS.map(
                new RichMapFunction<CommonTable, CommonTable>() {

                    private Connection hbaseconn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseconn = HBaseUtil.getHBaseConnection();

                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseconn);
                    }

                    @Override
                    public CommonTable map(CommonTable commonTable) throws Exception {
                        String op = commonTable.getOp();
                        //获取Hbase中维度表的表名
                        String sinkTable = commonTable.getSinkTable();
                        //获取在HBase中建表的列族
                        String[] sinkFamilies = commonTable.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据  将hbase中对应的表删除掉
                            HBaseUtil.dropHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                            HBaseUtil.createHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        else {
                            //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                            HBaseUtil.dropHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hbaseconn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return commonTable;
                    }
                });
//        tpDS.print(">>>>hbase输出");
//
        // 将配置流中的配置信息进行广播 -- broadcast
        MapStateDescriptor<String, CommonTable> mapStateDescriptor =
                new MapStateDescriptor<String, CommonTable>("mapStateDescriptor",String.class,CommonTable.class);
        BroadcastStream<CommonTable> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // 将主流业务数据和广播流配置信息进行关联
        BroadcastConnectedStream<JSONObject, CommonTable> connectDS = jsonObjDS.connect(broadcastDS);
//
//
        // 处理关联后的数据
        SingleOutputStreamOperator<Tuple2<JSONObject, CommonTable>> dimDS = connectDS.process(
                new Tablepeocessfcation(mapStateDescriptor)
        );

        dimDS.print(">>>>广播流");

        dimDS.addSink(new flinksinkHbase());
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
