package com.lxy.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxy.realtime.bean.TableProcessDwd;
import com.lxy.realtime.constant.Constant;
import com.lxy.realtime.function.BaseDbTableProcessFunction;
import com.lxy.realtime.utils.FlinkSinkUtil;
import com.lxy.realtime.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/6/06
 * 处理逻辑比较简单的事实表动态分流处理
 * 需要启动的进程
 *      zk、kafka、maxwell、DwdBaseDb
 *
 */
public class DwdBaseDb {

    @SneakyThrows
    public static void main(String[] args) {
        System.getProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dwd_log");

        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        //TODO 对流中的数据进行类型转换并进行简单的ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是一个标准的json");
                        }

                    }
                }
        );
        //jsonObjDS.print();

        //TODO 使用FlinkCDC读取配置表中的配置信息
        //创建MysqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.HBASE_NAMESPACE,"table_process_dwd");
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        //对流中数据进行类型转换   jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    TableProcessDwd tp = null;
                    @Override
                    public TableProcessDwd map(String jsonStr) {
                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取操作类型
                        String op = jsonObj.getString("op");
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
        //tpDS.print();

        //TODO 对配置流进行广播 ---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor",String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        //TODO 关联主流业务数据和广播流中的配置数据   --- connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
        //TODO 对关联后的数据进行处理   --- process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
        //TODO 将处理逻辑比较简单的事实表数据写到kafka的不同主题中
        //({"create_time":"2024-05-24 16:08:39","user_id":1261,"sku_id":26,"id":7554,"ts":1717667105},TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r))
        splitDS.print();
        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());

        env.execute();
    }
}
