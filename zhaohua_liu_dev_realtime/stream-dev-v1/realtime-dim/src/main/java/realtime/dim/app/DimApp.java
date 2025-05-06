package realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import realtime.common.base.BaseApp;
import realtime.common.bean.TableProcessDim;
import realtime.common.constant.Constant;
import realtime.common.util.FlinkSourceUtil;
import realtime.common.util.HbaseUtil;
import realtime.dim.function.HBaseSinkFunction;
import realtime.dim.function.TableProcessFunction;

/**
 * @Package realtime.dim.app.DimApp
 * @Author zhaohua.liu
 * @Date 2025/4/9.14:46
 * @description: DIM层运行位置
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(20002,4,"dim_app", Constant.TOPIC_ODS_INITIAL);
    }


    //todo 处理kafka读取的数据
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //读取e_commerce全量,并处理str类型为jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);
        //读取e_commerce_config数据库中的配置信息,并转为实体类,读取的是d的before,其他的after,即不管删不删都建表
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);
        //根据e_commerce_config中的配置信息到HBase中执行建表或者删除表操作
        tpDS = createHBaseTable(tpDS);
        // 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> DimDS = connect(tpDS, jsonObjDS);
        //将过滤后的数据写入hbase
        DimDS.print();
        DimDS.addSink(new HBaseSinkFunction());




    }

    //todo 从e_commerce全量读取中过滤出维度信息,准备添加到维度表
    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>>
    connect(SingleOutputStreamOperator<TableProcessDim> tpDS, SingleOutputStreamOperator<JSONObject> jsonObj){
        //mapStateDescriptor记录映射关系
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);

        //将配置流中的配置信息进行广播---broadcast
        //这里是将tpDS转为BroadcastStream<TableProcessDim> 类型的广播流
        //广播流中的元素存储在 mapStateDescriptor 所描述的映射状态里,即键的类型为 String，值的类型为 TableProcessDim
        //Flink 会在每个并行任务实例(流会根据并行度分成不同的子流,一个子流是一个并行任务实例)中创建一个广播状态,
        // 广播状态的生命周期和任务的生命周期是绑定的，只要flink程序没有停止，广播状态就会一直存在于内存中
        // 实时数据流中的每个元素就可以依据这个键快速查找对应的TableProcessDim
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //将主流业务数据和广播流配置信息进行关联---connect
        //connect 操作使两个流的数据能够交互和关联处理，但并没有改变它们各自的数据格式和存储方式
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObj.connect(broadcastDS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }




    //todo createHBaseTable方法
    //根据mysql的e_commerce_config数据在hbase中进行操作
    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS){
        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection connection;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //创建hbase连接
                        connection = HbaseUtil.getHbaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        //关闭连接,释放资源
                        HbaseUtil.closeHbaseConnection(connection);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //tp为mysql为dim层建hbase表的实体类
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取hbase维度表的表名
                        String sinkTable = tp.getSinkTable();
                        //获取hbase维度表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)) {
                            //从配置表中删除了一条数据  将hbase中对应的表删除掉
                            HbaseUtil.dropHbaseTable(connection,Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op) || "c".equals(op)){
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                            HbaseUtil.createHbaseTable(connection,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else {
                            //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                            HbaseUtil.dropHbaseTable(connection,Constant.HBASE_NAMESPACE,sinkTable);
                            HbaseUtil.createHbaseTable(connection,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        //返回的是对单个元素处理后的结果
                        return tp;
                    }
                }
        ).setParallelism(1);
        //单个元素会重新组合成一个新的数据流，最终由 createHBaseTable 方法返回
        return tpDS;
    }


    //todo readTableProcess方法
    //flinkCdc读取表配置信息,并转为实体类,并且d读取before,其他读取after
    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env){
        //读取e_commerce_config数据库,转为流
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("e_commerce_config", "table_process_dim");
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
        //流转实体类,并且d读取before,其他读取after
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.process(
                new ProcessFunction<String, TableProcessDim>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, TableProcessDim>.Context context, Collector<TableProcessDim> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim = null;
//                        c：代表创建（create），表示插入了一条新记录。
//                        u：代表更新（update），表示对已有记录进行了修改。
//                        d：代表删除（delete），表示删除了一条记录。
//                        r：代表读取（read），如果是一个 Snapshot（快照），则会使用这个值。
                        if ("d".equals(op)) {
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        //添加op字段
                        tableProcessDim.setOp(op);
                        collector.collect(tableProcessDim);
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    //todo etl方法
    //jsonStr->jsonObj
    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS){
        //返回一个JSONObject类型的数据流
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                JSONObject jsonObject = JSON.parseObject(s);
                String db = jsonObject.getJSONObject("source").getString("db");
                String op = jsonObject.getString("op");
                if ("e_commerce".equals(db) && ("c".equals(op) || "u".equals(op) || "d".equals(op) || "r".equals(op))
                ) {
                    collector.collect(jsonObject);
                }

            }
        });
        return jsonObj;
    }




}
