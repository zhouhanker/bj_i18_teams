package com.bg.realtime_dwd.base_db.APP;

import com.bg.common.base.BaseSQLApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.bg.realtime_dwd.base_db.app.DwdTradeCartAdd
 * @Author Chen.Run.ze
 * @Date 2025/4/11 9:08
 * @description: 加购事实表
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,1, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从Kafka的topic_db主题中读取数据 创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);

        //TODO 过滤出加购数据
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "if(op='c',`after`['sku_num'],CAST((CAST(`after`['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING )) sku_num, \n" +
                "`ts_ms`\n" +
                " from topic_db where `source`['table']='cart_info'  \n" +
                " and( \n" +
                " op = 'c' \n" +
                " or \n" +
                " op = 'r' \n" +
                " or \n" +
                " (op='u' and `before`['sku_num'] is not null and (CAST(`after`['sku_num'] AS INT) > CAST(`before`['sku_num'] AS INT))) \n" +
                ") ");
        //+----+-------+--------+--------+---------+---------------+
        //| op |    id |user_id | sku_id | sku_num |         ts_ms |
        //+----+-------+--------+--------+---------+---------------+
        //| +I |  2301 |     39 |     12 |       1 | 1745824055699 |
        //| +I |  2302 |    212 |     16 |       1 | 1745824055812 |
        //| +I |  2303 |    159 |      4 |       1 | 1745824055928 |
        //| +I |  2304 |    244 |     34 |       1 | 1745824055935 |
//        cartInfo.execute().print();

        //TODO 将过滤出来的加购数据写到Kafka主题中
        //创建动态表和要写入的主题映射
        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_TRADE_CART_ADD +" (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  sku_num STRING,\n" +
                "  ts_ms BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+ SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //写入
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }




}
