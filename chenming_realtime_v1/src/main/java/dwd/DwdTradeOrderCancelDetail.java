package dwd;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cm.dwd.DwdTradeOrderCancelDetail
 * @Author chen.ming
 * @Date 2025/4/11 19:00
 * @description: 取消订单表
 */
public class DwdTradeOrderCancelDetail {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
    //TODO 从kafka的topic_db主题中读取数据
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
//        +I[null, {callback_time=1744055899000, create_time=1744055877000, subject=小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机等1件商品, payment_status=1602, callback_content=callback xxxxxxx, payment_type=1102, out_trade_no=859427153439775, user_id=51, total_amount=6499.0, trade_no=null, id=122, order_id=127, operate_time=1744055899000}, {query=null, thread=null, server_id=0, version=1.9.7.Final, sequence=null, file=, connector=mysql, pos=0, name=mysql_binlog_source, gtid=null, row=0, ts_ms=0, snapshot=false, db=realtime_v1, table=payment_info}, r, 1744350157114, 2025-04-11T10:33:22.388Z]
        Table table = tenv.sqlQuery("select * from db ");
//        tenv.toChangelogStream(table).print();
        // TODO 过滤出取消订单行为
        Table orderCancel = tenv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " `ts_ms` as ts " +
                "from db " +
                "where `source`['table']='order_info' " );
        tenv.createTemporaryView("order_cancel", orderCancel);
//            tenv.toChangelogStream(orderCancel).print();
        // TODO 订单取消表和下单表进行 join
        tenv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "create_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts bigint" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_trade_order_detail_chenming',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

        Table result = tenv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(FROM_UNIXTIME(CAST(oc.operate_time AS BIGINT) / 1000), 'yyyy-MM-dd') AS order_cancel_date_id," +
                        "date_format(FROM_UNIXTIME(CAST(oc.operate_time AS BIGINT) / 1000), 'yyyy-MM-dd hh:mm:ss') AS operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");
        tenv.toChangelogStream(result).print();
        //TODO 将关联的结果写到kafka主题中
        tenv.executeSql(
                "create table dwd_trade_order_cancel(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'dwd_trade_order_cancel_chenming',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ");");
        result.executeInsert("dwd_trade_order_cancel");
        env.execute();
    }
}
