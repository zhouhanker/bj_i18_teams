package realtime.dwd.db.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.common.base.BaseSQLApp;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

/**
 * @Package realtime.dwd.db.app.DwdTradeCartAdd
 * @Author zhaohua.liu
 * @Date 2025/4/15.16:50
 * @description:加购表
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeCartAdd().start(20005,4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //从kafka的ods_initial主题中读取数据  创建动态表
        readOdsDb(tEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        //过滤出加购数据,table=cart_info,比上一次增加了多少数量
        Table cartAddTable = tEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['user_id'] user_id," +
                        "`after`['sku_id'] sku_id," +
                        "if(`op`='c',`after`['sku_num'],cast((cast(`after`['sku_num'] as int) - cast(`before`['sku_num'] as int)) as string)) sku_num," +
                        "`ts_ms`" +
                        " from ods_initial " +
                        " where `source`['table']='cart_info' "
                        +" and (" +
                        " `op`='c' " +
                        " or " +
                        " (`op`='u' and `after`['sku_num'] is not null and cast(`after`['sku_num'] as int) > cast(`before`['sku_num']as int))" +
                        ")"
        );
//        cartAddTable.execute().print();
        //将过滤出来的加购数据写到kafka主题中
        tEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                        "    id string,\n" +
                        "    user_id string,\n" +
                        "    sku_id string,\n" +
                        "    sku_num string,\n" +
                        "    ts bigint,\n" +
                        "    primary key (id) not enforced\n" +
                        ")"+ SQLutil.getUpsertKafkaDDl(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );
        //写入kafka
        cartAddTable.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
//        cartAddTable.execute().print();
    }
}
