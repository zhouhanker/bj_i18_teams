package dwd;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @Package com.cm.dwd.DwdTradeOrderDetail
 * @Author chen.ming
 * @Date 2025/4/11 15:30
 * @description: 订单表 订单明细(主表) 订单明细活动 订单明细优惠券 下单事实表
 */
public class DwdTradeOrderDetail {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

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
        //TODO 过滤出订单明细数据
        Table orderDetail = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['sku_name'] sku_name," +
                        "CAST(after['create_time'] as varchar )create_time," +
                        "after['source_id'] source_id," +
                        "after['source_type'] source_type," +
                        "after['sku_num'] sku_num," +
                        "CAST(cast(after['sku_num'] as decimal(16,2)) * " +
                        " CAST(after['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "after['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "after['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "after['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts_ms as ts " +
                        "from db " +
                        "where source['table']='order_detail' ");

        tenv.createTemporaryView("order_detail", orderDetail);
//        tenv.toChangelogStream(orderDetail).print("过滤出订单明细数据");
        //TODO 过滤出订单数据
        Table orderInfo = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from db " +
                        "where source['table']='order_info' ");
//        tenv.toChangelogStream(orderInfo).print();
        tenv.createTemporaryView("order_info", orderInfo);
//        tenv.toChangelogStream(orderInfo).print("过滤出订单数据");

        //TODO 过滤出明细活动数据
        Table orderDetailActivity = tenv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['activity_id'] activity_id, " +
                        "after['activity_rule_id'] activity_rule_id " +
                        "from db " +
                        "where source['table']='order_detail_activity'");
//        tenv.toChangelogStream(orderDetailActivity).print("过滤出明细活动数据");
        tenv.createTemporaryView("order_detail_activity", orderDetailActivity);
        //TODO 过滤出明细优惠券数据
        Table orderDetailCoupon = tenv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['coupon_id'] coupon_id " +
                        "from db " +
                        "where source['table']='order_detail_coupon'  ");
//        tenv.toChangelogStream(orderDetailCoupon).print(" 过滤出明细优惠券数据");
        tenv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
//TODO 关联上述4张表
        Table result = tenv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000), 'yyyy-MM-dd') AS date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");
//        tenv.toChangelogStream(result).print("关联表");

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
                "ts bigint," +
                "primary key(id) not enforced " +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_trade_order_detail_chenming',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        result.executeInsert("dwd_trade_order_detail");

        env.execute();
    }
}
