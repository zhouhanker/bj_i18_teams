package com.rb.dwd;

import com.rb.utils.DwdUtils;
import com.rb.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.rb.dwd.DwdCartAdd
 * @Author runbo.zhang
 * @Date 2025/4/11 16:24
 * @description:
 */
public class DwdCartAdd {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
//        System.setProperty("HADOOP_USER_NAME", "hdfs");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DwdUtils.dwdKafkaDbInit(tEnv, "log_topic_flink_online_v1_dwd");

        Table cartAdd = tEnv.sqlQuery("select " +
                " `after`['id']  id ,\n " +
                " `after`['user_id']  user_id ,\n " +
                " `after`['sku_id']  sku_id ,\n " +
                " if(op='c' ,`after`['sku_num'],cast(cast(`after`['sku_num'] as int)-cast(`before`['sku_num'] as int) as string) ) sku_num , \n" +
                " `source`['ts_ms']  ts \n" +
                " from topic_db" +
                " where `source`['table']='cart_info'" +
                " and (" +
                "       op='c' " +
                "       or " +
                "       op='u' and before is not null and  cast(`after`['sku_num'] as int)>cast(`before`['sku_num'] as int) " +
                ")"
        );
        tEnv.executeSql(" create table "+"dwd_cart_add"+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts string,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL("dwd_cart_add"));
        //写入
        cartAdd.executeInsert("dwd_cart_add");


    }

}
