package dwd;


import constant.Constant;
import util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cm.dwd.DwdTradeCartAdd
 * @Author chen.ming
 * @Date 2025/4/11 9:08
 * @description: 加购事实表
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //TODO 从Kafka的xinyi_jiao_yw主题中读取数据 创建动态表
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  `before` MAP<string,string>,\n" +
                "  `after` Map<String,String>,\n" +
                "   `source` Map<String,String>,\n" +
                "  `op`  String,\n" +
                "  `ts_ms`  bigint,\n" +
                "  `proc_time`  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'chenming_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        //TODO 过滤出加购数据
        Table cartInfo = tenv.sqlQuery("select \n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "if(op='c',`after`['sku_num'],CAST((CAST(`after`['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING )) sku_num, \n" +
                "`ts_ms`\n" +
                " from db where `source`['table']='cart_info'  \n" +
                " and( \n" +
                " op = 'c' \n" +
                " or \n" +
                " op = 'r' \n" +
                " or \n" +
                " (op='u' and `before`['sku_num'] is not null and (CAST(`after`['sku_num'] AS INT) > CAST(`before`['sku_num'] AS INT))) \n" +
                ") ");
        cartInfo.execute().print();

        //TODO 将过滤出来的加购数据写到Kafka主题中
        //创建动态表和要写入的主题映射
        tenv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_TRADE_CART_ADD +" (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  sku_num STRING,\n" +
                "  ts_ms BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+ SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //写入
        Table table = tenv.sqlQuery("select * from dwd_trade_cart_add_chenming");
        tenv.toChangelogStream(table).print();
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

    }


}
