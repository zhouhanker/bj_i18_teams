package com.dwd.realtime_dwd_trade_cart_add;


import com.common.base.BaseSQLApp;
import com.common.constant.Constant;
import com.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
加购实时表
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //todo 从kafka读取数据创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);

//        tableEnv.executeSql("select * from KafkaTable where `source`['table']='cart_info'");
        /*
`after`['id'],
`after`['user id'],
`after`['sku_id'],
`after`['cart_price'],
`after`['sku_num'],
`after`['img_url'],
`after`['sku_name'],
`after`['is_checked'],
`after`['create _time'],
`after``['operate_time'],
`after`['is_ordered'],
`after`['order time'],
         */
        Table cart_info = tableEnv.sqlQuery("select\n" +
                "`after`['id'],\n" +
                "`after`['user_id'],\n" +
                "`after`['sku_id'],\n" +
                "`after`['sku_num'],\n" +
                "ts_ms ts\n" +
                "from KafkaTable\n" +
                " where `source`['table'] = 'cart_info'");
//        cart_info.execute().print();



        //todo 过滤出加购数据 table='cart_info'
        tableEnv.executeSql(" create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts string,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //todo 将过滤出来的加购数据写入kafka主题中
        //创建动态变要和写入的主题进行映射
        //写入
        cart_info.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);


    }
}
