package realtime_Dim;

import Base.BaseApp;
import bean.CommonTable;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import constat.constat;
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
import realtime_Dim.flinkfcation.Tablepeocessfcation;
import realtime_Dim.flinkfcation.flinksorceutil;
import utils.Hbaseutli;
/**
 * @Package realtime_Dim.Dim_App
 * @Author ayang
 * @Date 2025/4/8 19:31
 * @description: 读取
 */
public class Dim_App extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dim_App().start(10001,4,"dim_app",constat.TOPIC_DB);
    }
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
//       kafkaStrDS.print();

        SingleOutputStreamOperator<JSONObject> kafkaDs = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                String db = jsonObj.getJSONObject("source").getString("db");
                String type = jsonObj.getString("op");
                String data = jsonObj.getString("after");
                if ("gmall_config".equals(db)
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
//        json-->> {"op":"c","after":{"birthday":1284,"create_time":1654646400000,"login_name":"63k5cvx","nick_name":"艺欣","name":"邹艺欣","user_level":"1","phone_num":"13512773463","id":42,"email":"63k5cvx@sohu.com"},"source":{"thread":49,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000006","connector":"mysql","pos":1572056,"name":"mysql_binlog_source","row":0,"ts_ms":1744418624000,"snapshot":"false","db":"realtime","table":"user_info"},"ts_ms":1744418621273}

//        kafkaDs.print("json-->");

        //cdc
        MySqlSource<String> getmysqlsource = flinksorceutil.getmysqlsource("gmall2025_config", "table_process_dim");
        DataStreamSource<String> mySQL_source = env.fromSource(getmysqlsource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);// 设置 sink 节点并行度为 1
//        mySQL_source.print();
        //"op":"r": {"before":null,"after":{"source_table":"activity_info","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1716812196180,"transaction":null}
        //"op":"c": {"before":null,"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"aa"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812267000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11423611,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1716812265698,"transaction":null}
        //"op":"u": {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"aa"},"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"aa"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812311000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11423960,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1716812310215,"transaction":null}
        //"op":"d": {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"aa"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812341000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11424323,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1716812340475,"transaction":null}
        SingleOutputStreamOperator<CommonTable> tpds = mySQL_source.map(new MapFunction<String, CommonTable>() {
            @Override
            public CommonTable map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String op = jsonObject.getString("op");
                CommonTable commonTable = null;
                if ("d".equals(op)) {
                    commonTable = jsonObject.getObject("before", CommonTable.class);
                } else {
                    commonTable = jsonObject.getObject("after", CommonTable.class);
                }
                commonTable.setOp(op);
                return commonTable;
            }
        });
//        2> CommonTable(sourceTable=base_trademark, sinkTable=dim_base_trademark, sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=c)

//        tpds.print();
        tpds.map(
                new RichMapFunction<CommonTable, CommonTable>() {

                    private Connection hbaseconn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseconn = Hbaseutli.getHBaseConnection();

                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutli.closeHBaseConnection(hbaseconn);
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
                            Hbaseutli.dropHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                            Hbaseutli.createHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        else {
                            //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                            Hbaseutli.dropHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable);
                            Hbaseutli.createHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return commonTable;
                    }
                });
        MapStateDescriptor<String, CommonTable> tableMapStateDescriptor = new MapStateDescriptor<>
                ("maps", String.class, CommonTable.class);
        BroadcastStream<CommonTable> broadcast = tpds.broadcast(tableMapStateDescriptor);

        BroadcastConnectedStream<JSONObject, CommonTable> connects = kafkaDs.connect(broadcast);
        //处理流合并
        SingleOutputStreamOperator<Tuple2<JSONObject, CommonTable>> dimDS = connects.process(
                new Tablepeocessfcation(tableMapStateDescriptor)
        );
//        2> ({"op":"u","dic_code":"1103","dic_name":"iiii"},CommonTable(sourceTable=base_dic, sinkTable=dim_base_dic, sinkColumns=dic_code,dic_name, sinkFamily=info, sinkRowKey=dic_code, op=c))

        dimDS.print();
//        dimDS.addSink(new flinksinkHbase());

    }
}

