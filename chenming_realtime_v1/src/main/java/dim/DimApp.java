package dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import bean.TableProcessDim;
import constant.Constant;
import dim.function.HBaseSinkFunction;
import dim.function.TableProcessFunction;
import util.FlinkSourceUtil;
import util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Package com.cm.dim.function.DimApp
 * @Author chen.ming
 * @Date 2025/4/8 08:52
 * @description: 1
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最大重启次数
                Time.seconds(5) // 重启间隔时间
        ));
        // 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置job取消后是否保留检查点
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置两个检查点最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 设置重启策咯
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        // 设置状态后端及其检查点储存路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/2207A/chenming/Flink");
        // 设置操作hadoop用户
        System.setProperty("HADOOP_USER_NAME","root");
        // TODO 3.从Kafka的topic_db主题中读取业务数据
        // 3.1 声明消费的主题以及消费者组
        String groupId = "dim_app_group";

        //3.2 创建消费者对象   优化 使用FlinkSourceUtil
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, groupId);
        //输出
        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
//        {"before":null,"after":{"id":16,"login_name":"h6ejz7","nick_name":"洁梅","passwd":null,"name":"顾洁梅","phone_num":"13148353964","email":"h6ejz7@msn.com","head_img":null,"user_level":"1","birthday":-785,"gender":"F","create_time":1654646400000,"operate_time":null,"status":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"dev_realtime_v1_xinyi_jiao","sequence":null,"table":"user_info","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744186877326,"transaction":null}
//        kafkaDs.print();
        //保证消费的精准一次性，需要手动维护偏移量
        //TODO 4.对业务流中数据类型进行转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject object = JSON.parseObject(s);
                String db = object.getJSONObject("source").getString("db");
                String type = object.getString("op");
                String data = object.getString("after");
                if ("realtime_v1".equals(db)
                        && ("c".equals(type)
                        ||"u".equals(type)
                        ||"d".equals(type)
                        ||"r".equals(type))
                        && data != null
                        && data.length() > 2
                ){
                  collector.collect(object);
                }
            }
        });
        jsonObjDS.print("===>主流");
//       {"op":"r","after":{"activity_name":"小米手机专场","start_time":1642035714000,"create_time":1653609600000,"activity_type":"3101","activity_desc":"小米手机满减2","end_time":1687132800000,"id":1},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"dev_realtime_v1_xinyi_jiao","table":"activity_info"},"ts_ms":1744097400031}
//       TODO5.使用FlinkCDC读取配置表中的配置信息

//       5.1创建MySQLSource对象
//       优化 使用工具类
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSourceUtil("realtime_v1_config", "table_process_dim");
        //读出数据 封装流
        DataStreamSource<String> ds1 = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
//          ds1.print("============>");
//{"before":null,"after":{"source_table":"coupon_range","sink_table":"dim_coupon_range","sink_family":"info","sink_columns":"id,coupon_id,range_type,range_id","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"config_all","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744111870600,"transaction":null}
// TOD06.对配置流中的数据类型进行转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDs = ds1.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                //为了方便 先转成jsonobj
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String op = jsonObject.getString("op");
                TableProcessDim tableProcessDim = null;
                if("d".equals(op)){
                    tableProcessDim= jsonObject.getObject("before",TableProcessDim.class);
                }else {
                    tableProcessDim= jsonObject.getObject("after",TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);
//      tpDs.print("封装数据==========>");
//      TableProcessDim(sourceTable=activity_info, sinkTable=dim_activity_info, sinkColumns=id,activity_name,activity_type,activity_desc,start_time,end_time,create_time, sinkFamily=info, sinkRowKey=id, op=r)

//      根据配置表中的配置信息到HBase中执行建表或者删除表操作
       tpDs= tpDs.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseCon;
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseCon = HBaseUtil.getHBaseConnection();
            }
            @Override
            public void close() throws Exception {
            HBaseUtil.closeHBaseConnection(hbaseCon);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) throws Exception {
                //获取一个对配置表的一个操作的类型
                String op = tp.getOp();
                //获取维度的表名
                String sinkTable = tp.getSinkTable();
                // 获取hbase建表的列组
                String[] splitFamily = tp.getSinkFamily().split(",");
                if("d".equals(op)){
                    //从配置表中删除了一条数据 从hbase中删除 对应的数据表
               HBaseUtil.dropHBaseTable(hbaseCon,Constant.HBASE_NAMESPACE,sinkTable);
                }else if("r".equals(op)||"c".equals(op)){
                    //从配置表中读取了一条数据或者向配置表中添加了一条配置 在hbase中执行建表
                    HBaseUtil.createHBaseTable(hbaseCon,Constant.HBASE_NAMESPACE,sinkTable,splitFamily);
                }else {
                    //对配置表中的配置信息进行了修改 先从hbase中将对应的表删除掉，再创建新表
                    HBaseUtil.dropHBaseTable(hbaseCon,Constant.HBASE_NAMESPACE,sinkTable);
                    HBaseUtil.createHBaseTable(hbaseCon,Constant.HBASE_NAMESPACE,sinkTable,splitFamily);
                }
                return tp;
            }
        });
//        表空间ns_chenming下的表dim_base_province创建成功
//        TableProcessDim(sourceTable=base_province, sinkTable=dim_base_province, sinkColumns=id,name,region_id,area_code,iso_code,iso_3166_2, sinkFamily=info, sinkRowKey=id, op=r)
        tpDs.print("ds==============>");
//
//
//
       // 将配置流中的配置信息进行广播  ---brodcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("MapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDs.broadcast(mapStateDescriptor);
        System.out.println(broadcastDS);
        //将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
        // 10.处理关联后的数据（判断是否为维度）


        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );

        dimDS.print("数据======================>");

        //将维度数据同步到HBase表中  //优化后
         dimDS.addSink(new HBaseSinkFunction());



        env.execute();
    }
     //过滤掉不需要用的字段
     //dataJson0bj {"tm_name":"Redmi", "create_time":"2021-12-14 00:00:00", "logo_url": "555", "id":1}
     //sinkColumns id,tm_name
    private static void deleteNeedColumns(JSONObject date, String sinkColumns) {
        List<String> coulumlist = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entries = date.entrySet();
         entries.removeIf(entry->!coulumlist.contains(entry.getKey()));
    }
}
