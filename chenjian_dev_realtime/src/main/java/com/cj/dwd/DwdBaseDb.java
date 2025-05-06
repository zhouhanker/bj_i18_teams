package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.bean.TableProcessDwd;
import com.cj.constant.Constant;
import com.cj.utils.FlinkSinkUtil;
import com.cj.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
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

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @Package com.cj.realtime.dwd.DwdBaseDb
 * @Author chen.jian
 * @Date 2025/4/13 19:06
 * @description: 事实表动态分流处理
 */
public class DwdBaseDb  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        读取kafka数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        kafkaStrDS.print();
//      判断数据是不是一条完整的json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            String type = jsonObj.getString("op");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是一个标准的json");
                        }
                    }
                }
        );
//        jsonObjDS.print();
//        读取维表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getmysqlsource("gmall2025_config", "table_process_dwd");
        DataStreamSource<String> mysqStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String s) throws Exception {

                        JSONObject object = JSON.parseObject(s);
                        String op = object.getString("op");
                        TableProcessDwd tp = null;
                        if ("d".equals(op)) {
                            tp = object.getObject("before", TableProcessDwd.class);
                        } else {
                            tp = object.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                }
        );
        tpDS.print();
//        TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r)
//        TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupor, op=r)
//        进行广播---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor",String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);
//        广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
//        处理关联后的数据(判断是否为维度)
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDwd>> splitDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>>() {
                    private Map<String,TableProcessDwd> configMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
                        String sql = "select * from gmall2025_config.table_process_dwd";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        while (rs.next()){
                            JSONObject jsonObj = new JSONObject();
                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                String columnName = metaData.getColumnName(i);
                                Object columnValue = rs.getObject(i);
                                jsonObj.put(columnName,columnValue);
                            }
                            TableProcessDwd tableProcessDim = jsonObj.toJavaObject(TableProcessDwd.class);
                            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                        }
                        rs.close();
                        ps.close();
                        conn.close();
                    }
                    private String getKey(String sourceTable, String sourceType) {
                        String key = sourceTable + ":" + sourceType;
                        return key;
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDwd>> out) throws Exception {

                        String table = jsonObj.getJSONObject("source").getString("table");

                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        TableProcessDwd tableProcessDim = broadcastState.get(table);
                        if (tableProcessDim != null){
                            JSONObject dataJsonObj = jsonObj.getJSONObject("after");
                            String sinkColumns = tableProcessDim.getSinkColumns();

                            deleteNoeedColumns(dataJsonObj,sinkColumns);
                            Long ts = jsonObj.getLong("ts_ms");
                            dataJsonObj.put("ts_ms",ts);
                            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
                        }

                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDwd>> out) throws Exception {
                        String op = tp.getOp();

                        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String sourceTable = tp.getSourceTable();
                        if ("d".equals(op)){
                            broadcastState.remove(sourceTable);
                            configMap.remove(sourceTable);
                        }else {
                            broadcastState.put(sourceTable,tp);
                            configMap.put(sourceTable,tp);
                        }
                    }
                }
        );
        splitDS.print();

//        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
//        splitDS.print();
////      将数据写入kafka
        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());
        env.execute();
    }
    private static void deleteNoeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        if (dataJsonObj == null) {
            return;
        }
        // 检查 sinkColumns 是否为 null
        if (sinkColumns == null) {
            return;
        }
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }

}
