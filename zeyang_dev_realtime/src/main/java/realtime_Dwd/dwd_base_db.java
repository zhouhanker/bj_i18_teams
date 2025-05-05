package realtime_Dwd;

import Base.BaseApp;
import bean.TableProcessDwd;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import constat.constat;
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
import realtime_Dim.flinkfcation.flinksorceutil;
import realtime_Dwd.function.BaseDbTableProcessFunction;

/**
 * @Package realtime_Dwd.dwd_base_db
 * @Author ayang
 * @Date 2025/4/14 11:40
 * @description: 事实表分流
 */
//数据已经跑了重新

public class dwd_base_db extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dwd_base_db().start(10008,1,"dim_app",constat.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中的数据进行类型转换并进行简单的ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out)   {
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

//        jsonObjDS.print();

        //TODO 使用FlinkCDC读取配置表中的配置信息
        //创建MysqlSource对象
        MySqlSource<String> mySqlSource = flinksorceutil.getmysqlsource("gmall2025_config","table_process_dwd");
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        //对流中数据进行类型转换   jsonStr->实体类对象
//        mysqlStrDS.print();

        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr)   {

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

//        tpDS.print();

        // 对配置流进行广播 ---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>
                ("mapStateDescriptor",String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);

//        // 关联主流业务数据和广播流中的配置数据   --- connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.
                connect(broadcastDS);
//        // 对关联后的数据进行处理   --- proces
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS =
                connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
        // 将处理逻辑比较简单的事实表数据写到kafka的不同主题中
        splitDS.print();

//        splitDS.sinkTo(finksink.getKafkaSink());



    }

}
