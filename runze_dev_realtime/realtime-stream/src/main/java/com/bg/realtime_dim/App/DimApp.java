package com.bg.realtime_dim.App;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.base.BaseApp;
import com.bg.common.bean.TableProcessDim;
import com.bg.common.constant.Constant;
import com.bg.common.util.FlinkSourceUtil;
import com.bg.common.util.HBaseUtil;
import com.bg.realtime_dim.function.HBaseSinkFunction;
import com.bg.realtime_dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.bg.realtime_dim.App.DimApp
 * @Author Chen.Run.ze
 * @Date 2025/4/7 22:31
 * @description: Dim维度层处理
 * DIM维度层的处理
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、HBase、DimApp
 * 开发流程
 *      基本环境准备
 *      检查点相关的设置
 *      从kafka主题中读取数据
 *      对流中数据进行类型转换并ETL     jsonStr->jsonObj
 *      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *      使用FlinkCDC读取配置表中的配置信息
 *      对读取到的配置流数据进行类型转换    jsonStr->实体类对象
 *      根据当前配置信息到HBase中执行建表或者删除操作
 *          op=d        删表
 *          op=c、r     建表
 *          op=u        先删表，再建表
 *      对配置流数据进行广播---broadcast
 *      关联主流业务数据以及广播流配置数据---connect
 *      对关联后的数据进行处理---process
 *          new TableProcessFunction extends BroadcastProcessFunction{
 *              open:将配置信息预加载到程序中，避免主流数据先到，广播流数据后到，丢失数据的情况
 *              processElement:对主流数据的处理
 *                  获取操作的表的表名
 *                  根据表名到广播状态中以及configMap中获取对应的配置信息，如果配置信息不为空，说明是维度，将维度数据发送到下游
 *                      Tuple2<dataJsonObj,配置对象>
 *                  在向下游发送数据前，过滤掉了不需要传递的属性
 *                  在向下游发送数据前，补充操作类型
 *              processBroadcastElement:对广播流数据进行处理
 *                  op =d  将配置信息从广播状态以及configMap中删除掉
 *                  op!=d  将配置信息放到广播状态以及configMap中
 *          }
 *     将流中数据同步到HBase中
 *          class HBaseSinkFunction extends RichSinkFunction{
 *              invoke:
 *                  type="delete" : 从HBase表中删除数据
 *                  type!="delete" :向HBase表中put数据
 *          }
 *     优化：抽取FlinkSourceUtil工具类
 *          抽取TableProcessFunction以及HBaseSinkFunction函数处理
 *          抽取方法
 *          抽取基类---模板方法设计模式
 * 执行流程（以修改了品牌维度表中的一条数据为例）
 *      当程序启动的时候，会将配置表中的配置信息加载到configMap以及广播状态中
 *      修改品牌维度
 *      binlog会将修改操作记录下来
 *      maxwell会从binlog中获取修改的信息，并封装为json格式字符串发送到kafka的topic_db主题中
 *      DimApp应用程序会从topic_db主题中读取数据并对其进行处理
 *      根据当前处理的数据的表名判断是否为维度
 *      如果是维度的话，将维度数据传递到下游
 *      将维度数据同步到HBase中
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001,4,"dim_app",Constant.TOPIC_DB);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //TODO 对业务流中数据类型进行转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaSource);

        //TODO 使用FlinkCDC读取配置信息表中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);

        //TODO 根据配置表中的配置信息到 HBase 中执行建表或者删除表操作
        tpDS = createHBaseTable(tpDS);

        //表空间gmall2024下的表dim_base_region创建成功
        tpDS.print("tpDS-->");

        //TODO 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDS, tpDS);

        //TODO 将维度数据同步到HBase表中
        //({"tm_name":"Redmi","op":"u","id":1},TableProcessDim(sourceTable=base_trademark, sinkTable=dim_base_trademark, sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=r))
        dimDS.print("dimDS-->");
        writeToHBase(dimDS);

    }

    private static void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        //TODO 8.将配置流中的配置信息进行广播--broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 9.将主流业务数据和广播流配置信息进行关联--connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        //TODO 10.处理关联后的数据（判断是否为维度）
        return connectDS.process(
                new TableProcessFunction(mapStateDescriptor)).setParallelism(1);
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

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
            public TableProcessDim map(TableProcessDim tp) {
                //获取对配置表获取的操作类型
                String op = tp.getOp();
                //获取HBase中维度表表明
                String sinkTable = tp.getSinkTable();
                //获取在HBase中建表的列族
                String[] sinkFamilies = tp.getSinkFamily().split(",");
                if ("d".equals(op)){
                    //从配置表中删除了一条数据 将HBase中对应的表删除
                    HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                }else if ("r".equals(op) || "c".equals(op)){
                    //从配置表中读取了一条数据或者添加了一条配置
                    HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                }else {
                    //从配置表中的配置进行了修改   先把HBase中对应的表删除再创建新表
                    HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                    HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                }
                return tp;
            }
        }).setParallelism(1);
        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        //创建MysqlSource
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall2024_config", "table_process_dim");
        //5.2 读取数据 封装为流
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);
//        {"before":null,"after":{"source_table":"activity_info","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744084103724,"transaction":null}
//        mySQLSource.print("config-->");


        //TODO 6.引配置流中的数据类型进行转换 json->实体类对象
        //      TableProcessDim(sourceTable=base_category3, sinkTable=dim_base_category3, sinkColumns=id,name,category2_id, sinkFamily=info, sinkRowKey=id, op=r)
//        tpDS.print();
        return mySQLSource.map((MapFunction<String, TableProcessDim>) s -> {
            JSONObject object = JSON.parseObject(s);
            String op = object.getString("op");
            TableProcessDim tableProcessDim;
            if ("d".equals(op)) {
                //对配置表进行一次删除操作  从before属性中获得删除前的配置信息
                tableProcessDim = object.getObject("before", TableProcessDim.class);
            } else {
                //对配置表进行了读取、添加、修改操作 从after属性中获取最新的配置信息
                tableProcessDim = object.getObject("after", TableProcessDim.class);
            }
            tableProcessDim.setOp(op);
            return tableProcessDim;
        }).setParallelism(1);
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSource) {

//        {"op":"c","after":{"order_status":"1002","create_time":1744061980000,"id":389,"order_id":191},"source":{"thread":280,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000002","connector":"mysql","pos":5083465,"name":"mysql_binlog_source","row":0,"ts_ms":1744033180000,"snapshot":"false","db":"gmall2024","table":"order_status_log"},"ts_ms":1744078989065}
//        jsonObjDS.print("json-->");
        return kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                JSONObject object = JSON.parseObject(s);
                String db = object.getJSONObject("source").getString("db");
                String type = object.getString("op");
                String data = object.getString("after");
                if ("gmall2024".equals(db)
                        && ("c".equals(type)
                        || "u".equals(type)
                        || "d".equals(type)
                        || "r".equals(type))
                        && data != null
                        && data.length() > 2
                ) {
//                    System.out.println(object);
                    collector.collect(object);
                }
            }
        });
    }
}
