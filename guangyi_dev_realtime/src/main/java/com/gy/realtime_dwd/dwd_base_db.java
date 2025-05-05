package com.gy.realtime_dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gy.Base.BaseApp;
import com.gy.bean.TableProcessDwd;
import com.gy.constat.constat;
import com.gy.realtime_dim.flinkfcation.flinksorceutil;
import com.gy.realtime_dwd.function.BaseDbTableProcessFunction;
import com.gy.utils.finksink;
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


/**
 * @Package realtime_Dwd.dwd_base_db
 * @Author guangyi_zhou
 * @Date 2025/4/14 11:40
 * @description: 事实表分流
 */

public class dwd_base_db extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dwd_base_db().start(10008,1,"dim_app", constat.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中的数据进行类型转换并进行简单的ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
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
//        ype\":\"sku_id\",\"pos_id\":8,\"pos_seq\":0},{\"item\":\"11\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":1},{\"item\":\"33\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":2},{\"item\":\"8\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":3},{\"item\":\"18\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":4},{\"item\":\"11\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":5},{\"item\":\"24\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":6},{\"item\":\"24\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":7},{\"item\":\"17\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":8},{\"item\":\"33\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":9},{\"item\":\"30\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":10},{\"item\":\"12\",\"item_type\":\"sku_id\",\"pos_id\":8,\"pos_seq\":11},{\"item\":\"35\",\"item_type\":\"sku_id\",\"pos_id\":9,\"pos_seq\":0},{\"item\":\"9\",\"item_type\":\"sku_id\",\"pos_id\":9,\"pos_seq\":1}],\"page\":{\"during_time\":5217,\"page_id\":\"activity1111\",\"refer_id\":\"3\"},\"ts\":1743911070217}","id":13182},"source":{"thread":513,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000001","connector":"mysql","pos":17105308,"name":"mysql_binlog_source","row":8,"ts_ms":1744112096000,"snapshot":"false","db":"realtime","table":"z_log"},"ts_ms":1744554506139}

//        jsonObjDS.print("etl:");

        //TODO 使用FlinkCDC读取配置表中的配置信息
        //创建MysqlSource对象
        MySqlSource<String> mySqlSource = flinksorceutil.getmysqlsource("stream_retail_config","table_process_dwd");
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        //对流中数据进行类型转换   jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr) throws Exception {
                        //"op":"r": {"before":null,"after":{"source_table":"activity_info","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1716812196180,"transaction":null}
                        //"op":"c": {"before":null,"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"aa"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812267000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11423611,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1716812265698,"transaction":null}
                        //"op":"u": {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"aa"},"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"aa"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812311000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11423960,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1716812310215,"transaction":null}
                        //"op":"d": {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"aa"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812341000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11424323,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1716812340475,"transaction":null}

                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
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
//        TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r)

        tpDS.print("实体类");

        //TODO 对配置流进行广播 ---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor",String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);

//        //TODO 关联主流业务数据和广播流中的配置数据   --- connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
//        //TODO 对关联后的数据进行处理   --- process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
////        //TODO 将处理逻辑比较简单的事实表数据写到kafka的不同主题中
////        //({"create_time":"2024-05-24 16:08:39","user_id":1261,"sku_id":26,"id":7554,"ts":1717667105},TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r))
        splitDS.print("广播流");
        splitDS.sinkTo(finksink.getKafkaSink());



    }

}
