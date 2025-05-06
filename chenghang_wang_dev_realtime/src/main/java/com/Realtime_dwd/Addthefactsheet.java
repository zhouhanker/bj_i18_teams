package com.Realtime_dwd;

import com.Base.BasesqlApp;
import com.Constat.constat;
import com.utils.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package realtime_Dwd.Addthefactsheet
 * @Author ayang
 * @Date 2025/4/11 14:49
 * @description: 加购事实表
 */
//数据已经重新跑了
public class Addthefactsheet extends BasesqlApp {
    public static void main(String[] args) {
        new Addthefactsheet().start(10003,4,"dwd_Addthefactsheet");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        readOdsDb(tableEnv, constat.TOPIC_DWD_TRADE_CART_ADD);
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `after`['id'] id,\n" +
                "   `after`['user_id'] user_id,\n" +
                "   `after`['sku_id'] sku_id,\n" +
                "   if(op='c',`after`['sku_num'], " +
                "CAST((CAST(after['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
                "   ts_ms\n" +
                "from topic_table_v1 \n" +
                "where source['table']='cart_info' \n" +
                "and (\n" +
                "    op = 'c'\n" +
                "    or\n" +
                "    (op='u' and `before`['sku_num'] is not null and (CAST(after['sku_num'] AS INT) > CAST(`before`['sku_num'] AS INT)))\n" +
                ")");
        cartInfo.execute().print();
        tableEnv.executeSql(" create table "+constat.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + Sqlutil.getUpsertKafkaDDL(constat.TOPIC_DWD_TRADE_CART_ADD));
        //写入
        cartInfo.executeInsert(constat.TOPIC_DWD_TRADE_CART_ADD);


    }
}
