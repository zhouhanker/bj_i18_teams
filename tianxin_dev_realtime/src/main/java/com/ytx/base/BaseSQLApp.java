package com.ytx.base;


import com.ytx.constant.Constant;
import com.ytx.util.Sqlutil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public abstract class BaseSQLApp {
    public  void start(int port,int parallelism,String ck) {
        Configuration conf= new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // enable checkpoint
        env.setParallelism(parallelism);
//        env.enableCheckpointing(3000);
//        开启检查点
       env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//    设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        handle(tableEnv);
    }

    public abstract void handle(StreamTableEnvironment tableEnv);

    public void readOdsDb(StreamTableEnvironment tableEnv,String groupId) {
        tableEnv.executeSql("CREATE TABLE topic_db_yue (\n" +
                        "  `op` string,\n" +
                        "  `before` MAP<string,string>,\n" +
                        " `after` MAP<string,string>,\n" +
                        "  `source` MAP<string,string>,\n" +
                        " `ts_ms` bigint,\n" +
                        "`pt` as proctime(),\n" +
                        "`et` as to_timestamp_ltz(ts_ms,0),\n" +
                        "watermark for et as et - interval '3' second \n" +
                        ")" + Sqlutil.getKafkaDDL(Constant.TOPIC_DB,groupId));
        //tableEnv.executeSql("select * from topic_db").print();
    }

    //从Hbase的字典表中读取数据，创建动态表
    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + Sqlutil.getHBaseDDL("dim_base_dic"));
        //tableEnv.executeSql("select * from base_dic").print();
    }
}
