package com.jl.dwd;



import com.jl.constant.Constant;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.jl.DwdTradeCartAdd
 * @Author jia.le
 * @Date 2025/4/11 20:49
 * @description: DwdTradeCartAdd
 */

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
        // flink sql初始化
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka的topic_db主题中读取数据 创建动态表       ---kafka连接器
        tableEnv.executeSql("create table topic_db(\n" +
                "    `before` map<string,string>,\n" +
                "    `after` map<string,string>,\n" +
                "    `source` map<string,string>,\n" +
                "    `op` string,\n" +
                "    `ts_ms` BIGINT,\n" +
                "    proc_time as proctime()\n" +
                ")WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from topic_db").print();

        //TODO 过滤出加购数据  table='cart_info' type='insert' 、type = 'update' 并且修改的是加购商品的数量，修改后的值大于修改前的值
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `after`['id'] id,\n" +
                "   `after`['user_id'] user_id,\n" +
                "   `after`['sku_id'] sku_id,\n" +
                "   if(op='c',`after`['sku_num'], CAST((CAST(after['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING)) sku_num, \n" +
                "   `source`['ts_ms'] ts_ms\n" +
                "from topic_db \n" +
                "where `source`['table']='cart_info' \n" +
                "and (\n" +
                "    op = 'c'\n" +
                "    or\n" +
                "    (op='u' and `before`['sku_num'] is not null and (CAST(`after`['sku_num'] AS INT) >= CAST(`before`['sku_num'] AS INT)))\n" +
                ")");
//        tableEnv.createTemporaryView("cartInfo",cartInfo);
//        cartInfo.execute().print();


        //TODO 将过滤出来的加购数据写到kafka主题中
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql(" create table dwd_trade_cart_add(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts_ms string,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " ) WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_trade_cart_add',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        //写入
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

//        env.execute("dwd_kafka");
    }
}
