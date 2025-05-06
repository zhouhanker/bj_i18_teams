package realtime.dwd.db.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.common.base.BaseSQLApp;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

/**
 * @Package realtime.dwd.db.app.DwdTradeOrderRefund
 * @Author zhaohua.liu
 * @Date 2025/4/18.8:50
 * @description:
 */
public class DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeOrderRefund().start(20011,4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //读取kafka中的ods_initial
        readOdsDb(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
        //读取新增的order_refund_info
        Table OrderRefundInfoTable = tEnv.sqlQuery(
                "select " +
                        " `after`['id'] id, " +
                        " `after`['user_id'] user_id, " +
                        " `after`['order_id'] order_id, " +
                        " `after`['sku_id'] sku_id, " +
                        " `after`['refund_type'] refund_type, " +
                        " `after`['refund_num'] refund_num, " +
                        " `after`['refund_amount'] refund_amount, " +
                        " `after`['refund_reason_type'] refund_reason_type, " +
                        " `after`['refund_reason_txt'] refund_reason_txt, " +
                        " `after`['create_time'] create_time," +
                        " `ts_ms` as ts," +
                        "  `pt`" +
                        " from ods_initial " +
                        " where `source`['table']='order_refund_info' " +
                        " and `op`='c'"
        );
        tEnv.createTemporaryView("order_refund_info",OrderRefundInfoTable);
        //读取order_info中退款中的数据
        Table OrderInfoTable = tEnv.sqlQuery(
                "select `after`['id'] id, `after`['province_id'] province_id " +
                        "from ods_initial" +
                        " where `source`['table']='order_info' " +
                        " and `op`='u' " +
                        " and `before`['order_status'] is not null " +
                        " and `after`['order_status']='1005'"
        );
        tEnv.createTemporaryView("order_info",OrderInfoTable);
        //读取base_dic中的数据
        readHbaseDic(tEnv);
        //order_refund_info与base_dic使用lookupJoin,与order_info使用普通join
        Table joinTable = tEnv.sqlQuery(
                "select\n" +
                        "    ri.id,\n" +
                        "    ri.user_id,\n" +
                        "    ri.order_id,\n" +
                        "    ri.sku_id,\n" +
                        "    oi.province_id,\n" +
                        "    date_format(to_timestamp_ltz(cast(ri.create_time as bigint),3),cast('yyyy-MM-dd' as string)),\n" +
                        "    ri.create_time,\n" +
                        "    ri.refund_type,\n" +
                        "    dic1.dic_name,\n" +
                        "    ri.refund_reason_type,\n" +
                        "    dic2.dic_name,\n" +
                        "    ri.refund_reason_txt,\n" +
                        "    ri.refund_num,\n" +
                        "    ri.refund_amount,\n" +
                        "    ri.ts\n" +
                        "from order_refund_info ri\n" +
                        "join order_info oi on ri.order_id=oi.id\n" +
                        "join base_dic for system_time as of ri.pt as dic1\n" +
                        "on ri.refund_type=dic1.dic_code\n" +
                        "join base_dic for system_time as of ri.pt as dic2\n" +
                        "on ri.refund_reason_type=dic2.dic_code"
        );
        //创建upsertKafka表
        tEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(\n" +
                        "    id string,\n" +
                        "    user_id string,\n" +
                        "    order_id string,\n" +
                        "    sku_id string,\n" +
                        "    province_id string,\n" +
                        "    date_id  string,\n" +
                        "    create_time string,\n" +
                        "    refund_type string,\n" +
                        "    refund_name string,\n" +
                        "    refund_reason_type string,\n" +
                        "    refund_reason_name string,\n" +
                        "    refund_reason_txt string,\n" +
                        "    refund_num string,\n" +
                        "    refund_amount string,\n" +
                        "    ts bigint,\n" +
                        "    primary key (id) not enforced \n" +
                        ")"+ SQLutil.getUpsertKafkaDDl(Constant.TOPIC_DWD_TRADE_ORDER_REFUND)
        );
        //写入kafka
        joinTable.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

//        tEnv.executeSql("select * from "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND).print();
    }
}
