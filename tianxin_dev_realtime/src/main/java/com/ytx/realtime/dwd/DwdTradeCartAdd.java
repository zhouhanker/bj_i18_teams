package com.ytx.realtime.dwd;


import com.ytx.base.BaseSQLApp;
import com.ytx.constant.Constant;
import com.ytx.util.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/*
交易域加购事务事实表
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10007,4, Constant.TOPIC_DB);
    }


    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv,Constant.TOPIC_DB);
//        过滤出加购数据
        Table cartInfo = tableEnv.sqlQuery("select\n" +
                " `after`['id'] id,\n" +
                " `after`['user_id'] user_id,\n" +
                " `after`['sku_id'] sku_id,\n" +
                " if(op='c',`after`['sku_num'],cast((cast(`after`['sku_num'] as int)-\n" +
                "cast(`before`['sku_num']as int)\n" +
                ")as string)) sku_num,\n" +
                "ts_ms\n" +
                "from topic_db_yue where source['table']='cart_info'\n" +
                "and (op='c' or (op='u' and `before`['sku_num'] is not null and (cast(`after`['sku_num'] as int)>\n" +
                "cast(`before`['sku_num'] as int)))\n" +
                ")");
       cartInfo.execute().print();
//         过滤出加购数据写到kafka主题
        tableEnv.executeSql(" create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + Sqlutil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //写入
//        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
