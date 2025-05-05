package realtime.dwd.db.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.common.base.BaseSQLApp;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

import java.time.Duration;

/**
 * @Package realtime.dwd.db.app.DwdTradeOrderDetail
 * @Author zhaohua.liu
 * @Date 2025/4/15.20:57
 * @description: 订单事实表
 */
public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeOrderDetail().start(20006,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //设置状态的保留时间[传输的延迟 + 业务上的滞后关系]
        //在多表 JOIN 操作里，Flink 会维护中间状态以存储参与 JOIN 的表的数据。
        // 例如，在进行双流 JOIN 时，Flink 需要把每个流的数据状态保存起来，直到找到匹配的记录。
        // 如果数据量很大或者某些键的数据长时间没有新记录，这些状态会不断累积，从而占用大量的内存资源。
        //设置了空闲状态保留时间为 10 秒，Flink 会自动清理空闲超过 10 秒的状态，从而节省内存资源，提高系统性能。
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        //从kafka的topic_db主题中读取数据 创建动态表
        readOdsDb(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        //过滤出订单详情表
        Table orderDetailTable = tEnv.sqlQuery(
                "select\n" +
                        "    `after`['id'] id,\n" +
                        "    `after`['order_id'] order_id,\n" +
                        "    `after`['sku_id'] sku_id,\n" +
                        "    `after`['sku_name'] sku_name,\n" +
                        "    `after`['create_time'] create_time,\n" +
                        "    `after`['source_id'] source_id,\n" +
                        "    `after`['source_type'] source_type,\n" +
                        "    `after`['sku_num'] sku_num,\n" +
                        "    cast((cast(`after`['sku_num'] as decimal(16,2)) * cast(`after`['order_price'] as decimal(16,2))) as string) split_original_amount,\n" +
                        "    `after`['split_total_amount'] split_total_amount,\n" +
                        "    `after`['split_activity_amount'] split_activity_amount,\n" +
                        "    `after`['split_coupon_amount'] split_coupon_amount,\n" +
                        "    `ts_ms`  ts\n" +
                        "from ods_initial\n" +
                        "where `source`['table']='order_detail'\n" +
                        "and (`op`='c' or `op`='r')"
        );
        tEnv.createTemporaryView("order_detail",orderDetailTable);
        //过滤出订单表
        Table orderInfoTable = tEnv.sqlQuery(
                "select \n" +
                        "    `after`['id'] id,\n" +
                        "    `after`['user_id'] user_id,\n" +
                        "    `after`['province_id'] province_id\n" +
                        "from ods_initial\n" +
                        "where `source`['table']='order_info'\n" +
                        "and (`op`='r' or `op`='c')\n"
        );
        tEnv.createTemporaryView("order_info",orderInfoTable);
        //过滤出明细活动数据
        Table orderDetailActivityTable = tEnv.sqlQuery(
                "select\n" +
                        "    `after`['order_detail_id'] order_detail_id,\n" +
                        "    `after`['activity_id'] activity_id,\n" +
                        "    `after`['activity_rule_id'] activity_rule_id\n" +
                        "from ods_initial\n" +
                        "where `source`['table']='order_detail_activity'\n" +
                        "and (`op`='r' or `op`='c')"
        );
        tEnv.createTemporaryView("order_detail_activity",orderDetailActivityTable);
        //过滤出明细优惠券
        Table OrderDetailCouponTable = tEnv.sqlQuery(
                "select\n" +
                        "    `after`['order_detail_id'] order_detail_id,\n" +
                        "    `after`['coupon_id'] coupon_id\n" +
                        "from ods_initial\n" +
                        "where `source`['table']='order_detail_coupon'\n" +
                        "and (`op`='r' or `op`='c')"
        );
        tEnv.createTemporaryView("order_detail_coupon",OrderDetailCouponTable);

        //关联上述四张表
        Table joinTable = tEnv.sqlQuery(
                "select\n" +
                        "    od.id,\n" +
                        "    od.order_id,\n" +
                        "    oi.user_id,\n" +
                        "    od.sku_id,\n" +
                        "    od.sku_name,\n" +
                        "    oi.province_id,\n" +
                        "    act.activity_id,\n" +
                        "    act.activity_rule_id,\n" +
                        "    cou.coupon_id,\n" +
                        "    DATE_FORMAT(TO_TIMESTAMP_LTZ(cast(od.create_time as bigint),3),cast('yyyy-MM-dd' as string)) date_id,\n" +
                        "    od.create_time,\n" +
                        "    od.sku_num,\n" +
                        "    od.split_original_amount,\n" +
                        "    od.split_activity_amount,\n" +
                        "    od.split_coupon_amount,\n" +
                        "    od.split_total_amount,\n" +
                        "    od.ts\n" +
                        "from order_detail od\n" +
                        "join order_info oi on od.order_id = oi.id\n" +
                        "left join order_detail_activity act on act.order_detail_id = od.id\n" +
                        "left join order_detail_coupon cou on cou.order_detail_id = od.id"
        );
        //将映射kafka表
        tEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
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
                        ")" + SQLutil.getUpsertKafkaDDl(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));




        //写入
        joinTable.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        //写入和读取不能一起,否则报错
//      tEnv.executeSql("select * from dwd_trade_order_detail").print();
    }
}
