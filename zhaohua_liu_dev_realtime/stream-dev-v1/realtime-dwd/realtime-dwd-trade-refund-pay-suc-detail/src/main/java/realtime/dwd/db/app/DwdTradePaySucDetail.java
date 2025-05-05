package realtime.dwd.db.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.common.base.BaseSQLApp;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

/**
 * @Package realtime.dwd.db.app.DwdTradePaySucDetail
 * @Author zhaohua.liu
 * @Date 2025/4/16.21:42
 * @description: 退单成功表
 */
public class DwdTradePaySucDetail extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradePaySucDetail().start(20009,4, Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL);
    }
    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //读取ods_initial
        readOdsDb(tEnv,Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL);
        //读取字典表
        readHbaseDic(tEnv);
        //过滤出退单表中退单完成的数据
        Table orderRefundInfoTable = tEnv.sqlQuery(
                "select\n" +
                        "    `after`['order_id'] order_id,\n" +
                        "    `after`['sku_id'] sku_id,\n" +
                        "    `after`['refund_num'] refund_num\n" +
                        "from ods_initial\n" +
                        "where\n" +
                        "`source`['table']='order_refund_info'\n" +
                        "and `op`='u'\n" +
                        "and `before`['refund_status'] is not null\n" +
                        "and `after`['refund_status']='0705';"
        );
        tEnv.createTemporaryView("order_refund_info",orderRefundInfoTable);

        //过滤出退款支付表中退款成功数据
        Table refundStatusTable = tEnv.sqlQuery(
                "select\n" +
                        "    `after`['id'] id,\n" +
                        "    `after`['order_id'] order_id,\n" +
                        "    `after`['sku_id'] sku_id,\n" +
                        "    `after`['payment_type'] payment_type,\n" +
                        "    `after`['callback_time'] callback_time,\n" +
                        "    `after`['total_amount'] total_amount,\n" +
                        "    `pt`,\n" +
                        "    `ts_ms` as ts\n" +
                        "from ods_initial\n" +
                        "where `source`['table']='refund_payment'\n" +
                        "and `op`='u'\n" +
                        "and `before`['refund_status'] is not null\n" +
                        "and `after`['refund_status']='1602';"
        );
//        refundStatusTable.execute().print();
        tEnv.createTemporaryView("refund_payment",refundStatusTable);

        //过滤出订单表中退款完成的数据
        Table orderInfoTable = tEnv.sqlQuery(
                "select\n" +
                        "    `after`['id'] id,\n" +
                        "    `after`['user_id'] user_id,\n" +
                        "    `after`['province_id'] province_id\n" +
                        "from ods_initial\n" +
                        "where `source`['table']='order_info'\n" +
                        "and `op`='u'\n" +
                        "and `before`['order_status'] is not null\n" +
                        "and `after`['order_status']='1006'"
        );
        tEnv.createTemporaryView("order_info",orderInfoTable);
//        orderInfoTable.execute().print();

        //四表join,与base_dic的为lookupJoin
        Table joinTable = tEnv.sqlQuery(
                "select\n" +
                        "    rp.id,\n" +
                        "    oi.user_id,\n" +
                        "    rp.order_id,\n" +
                        "    rp.sku_id,\n" +
                        "    oi.province_id,\n" +
                        "    rp.payment_type,\n" +
                        "    dic.info.dic_name payment_type_name,\n" +
                        "    date_format(to_timestamp_ltz(cast(rp.callback_time as bigint),3),'yyyy-MM-dd') date_id,\n" +
                        "    rp.callback_time,\n" +
                        "    ori.refund_num,\n" +
                        "    rp.total_amount,\n" +
                        "    rp.ts\n" +
                        "from refund_payment rp\n" +
                        "join order_refund_info ori\n" +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id\n" +
                        "join order_info oi\n" +
                        "on rp.order_id=oi.id\n" +
                        "join base_dic for system_time as of rp.pt as dic\n" +
                        "on rp.payment_type=dic.dic_code"
        );

        //创建upsertKafka映射表
        tEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL+"(\n" +
                        "    refund_payment_id string,\n" +
                        "    user_id string,\n" +
                        "    order_id string,\n" +
                        "    sku_id string,\n" +
                        "    province_id string,\n" +
                        "    payment_type string,\n" +
                        "    payment_type_name string,\n" +
                        "    date_id string,\n" +
                        "    callback_time string,\n" +
                        "    refund_num string,\n" +
                        "    total_amount string,\n" +
                        "    ts bigint," +
                        "    primary key (refund_payment_id) not enforced)"+ SQLutil.getUpsertKafkaDDl(Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL)
        );

        //写入kafka
        joinTable.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL);

//        tEnv.executeSql("select * from dwd_trade_refund_pay_suc_detail").print();





    }

}
