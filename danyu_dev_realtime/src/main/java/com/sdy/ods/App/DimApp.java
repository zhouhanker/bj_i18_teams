package com.sdy.ods.App;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdy.bean.HBaseUtil;
import com.sdy.bean.TableProcessDim;
import com.sdy.domain.Constant;
import com.sdy.ods.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Properties;

/**
 * @Package com.sdy.retail.v1.realtime.App.DimApp
 * @Author danyu-shi
 * @Date 2025/4/8 15:29
 * @description:
 */
public class DimApp {
    @SneakyThrows
    public static void main(String[] args){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.enableCheckpointing(60000);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_LOG)
                .setGroupId("flink_consumer_group")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null){
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                        .build();

        DataStreamSource<String> kafkastrdev2 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkastrdev2.process(new ProcessFunction<String, JSONObject>() {@Override public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
            JSONObject jsonObj = JSON.parseObject(s);
//            String db = jsonObj.getString("database");
//            String type = jsonObj.getString("type");
//            String data = jsonObj.getString("data");

//            if ("gmall2025".equals(db)
//                    && ("insert".equals(type)
//                    || "update".equals(type)
//                    || "delete".equals(type)
//                    || "bootstrap-insert".equals(type))
//                    && data != null
//                    && data.length() > 2) {
                collector.collect(jsonObj);
//            }
        }}
        );

//        jsonObjDS.print();


        Properties prop = new Properties();
        prop.put("useSSL","false");
        prop.put("decimal.handling.mode","double");
        prop.put("time.precision.mode","connect");
        prop.setProperty("scan.incremental.snapshot.chunk.key-column", "id");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.160.60.17")
                .port(3306)
                .databaseList("realtime_v1_config") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("realtime_v1_config.table_process_dim") // 设置捕获的表
                .username("root")
                .password("Zh1028,./")
//                .startupOptions(StartupOptions.initial())  // 从最早位点启动
               .startupOptions(StartupOptions.latest()) // 从最晚位点启动
                .debeziumProperties(prop)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStream<String> mySQLSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        SingleOutputStreamOperator<TableProcessDim> tpDS = mySQLSource.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String s) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
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

                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);

                        } else if ("r".equals(op) || "c".equals(op)) {

                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);

                        } else {
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);

                        }

                        return tp;
                    }
                }
        );


        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject,TableProcessDim> coonectDS = jsonObjDS.connect(broadcastDS);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = coonectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );

        dimDS.print();
//        dimDS.addSink(new INHBase());


        env.execute();
    }

}