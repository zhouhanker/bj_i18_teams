package com.dwd.realtime_dwd_base_db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.Bean.TableProcessDwd;
import com.common.base.BaseApp;
import com.common.utils.Constat;
import com.common.utils.FlinkSinkUtil;
import com.common.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DwdBaseDb extends BaseApp {

    public static void main(String[] args) {
        new DwdBaseDb().start(10019,4,"dwd_base_db", Constat.TOPIC_DB,"source05");
    }

    @Override
    public void Handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceDS) {
            //TOD0 对流中的数据进行类型转换并进行简单的ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDS.process(

                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                        try {
//                            JSONObject jsonObject = JSON.parseObject(jsonStr);
//                            String op = jsonObject.getString("op");
//                            if (!op.startsWith("bootstrap-")) {
//                                out.collect(jsonObject);
//                            }
//                        } catch (Exception e) {
//                            throw new RuntimeException("不是标准json");
//                        }
                        boolean valid = JSON.isValid(jsonStr);
                        if (valid) {
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            String op = jsonObject.getString("op");
                            if (!op.startsWith("bootstrap-")) {
                                out.collect(jsonObject);
                            }
                        }

                    }
                }
        );
//        jsonObjDS.print();
            //ToDo 使用FlinkcDc读取配置表中的配置信息
        // 创道MysqlSource对象读取数据 封装为流
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1","table_process_dwd");
        DataStreamSource<String> mysqlstrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source1");
            //对流中数据进行类型转换jsonStr->史体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlstrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jaonStr) throws Exception {
                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jaonStr);
                        //获取操作类型
                        String op = jsonObj.getString("op");
                        TableProcessDwd tp = null;
                        if("d".equals(op)){
                            //对配置表进行了删除操作   需要从before属性中获取删除前配置信息
                            tp = jsonObj.getObject("before", TableProcessDwd.class);
                        }else{
                            //对配置表进行了读取、插入、更新操作   需要从after属性中获取配置信息
                            tp = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                }
        );
//        tpDS.print();
//        v3> TableProcessDwd(sourceTable=user_info, sourceType=c, sinkTable=dwd_user_register, sinkColumns=id,create_time, op=r)
        //TOD0 对配胃流进行广播---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor",String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        //TOD0 关联主流业务数据和广播流中的配置数据--- connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
            //TOD0 对关联后的数据进行处理---process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));

        //TOD0 将处理逻辑比较简单的事实表数据写到kafka的不同主题中
        splitDS.print();
        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
