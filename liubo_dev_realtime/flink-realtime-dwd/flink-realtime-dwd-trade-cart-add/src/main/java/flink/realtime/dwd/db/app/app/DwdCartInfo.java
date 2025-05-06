package flink.realtime.dwd.db.app.app;

import com.struggle.flink.realtime.common.base.BaseSQLApp;
import com.struggle.flink.realtime.common.constant.Constant;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

;

/**
 * @version 1.0
 * @Package flink.realtime.dwd.db.app.DwdCartInfo
 * @Author liu.bo
 * @Date 2025/5/3 16:41
 * @description: 订单事实表
 */
public class DwdCartInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdCartInfo().start(
                10015,
                4
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);
        Table cart_info = tableEnv.sqlQuery("select\n" +
                "\t`after`['id'] id,\n" +
                "\t`after`['user_id'] user_id,\n" +
                "\t`after`['sku_id'] sku_id,\n" +
                "\t`after`['sku_num'] sku_num,\n" +
                "\t`ts_ms`" +
                "from topic_db where source['table']='cart_info'\n" +
                "and op='c'");
        //将cart_info写入到kafka
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(" +
                        "id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_num string," +
                        "ts bigint," +
                        "primary key(id) not enforced " +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = '" + Constant.TOPIC_DWD_TRADE_CART_ADD + "',\n" +
                        "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")");
        //写入
        cart_info.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
