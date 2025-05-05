package realtime.dwd.db.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.common.base.BaseSQLApp;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

/**
 * @Package realtime.dwd.db.app.DwdTradeOrderPaySucDetail
 * @Author zhaohua.liu
 * @Date 2025/4/16.15:22
 * @description: 支付成功表
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeOrderPaySucDetail().start(20008,4, Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL);
    }
    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //创建kafka映射表,读取订单明细表
        tEnv.executeSql(
                "create table dwd_trade_order_detail\n" +
                        "(\n" +
                        "    id                    string,\n" +
                        "    order_id              string,\n" +
                        "    user_id               string,\n" +
                        "    sku_id                string,\n" +
                        "    sku_name              string,\n" +
                        "    province_id           string,\n" +
                        "    activity_id           string,\n" +
                        "    activity_rule_id      string,\n" +
                        "    coupon_id             string,\n" +
                        "    date_id               string,\n" +
                        "    create_time           string,\n" +
                        "    sku_num               string,\n" +
                        "    split_original_amount string,\n" +
                        "    split_activity_amount string,\n" +
                        "    split_coupon_amount   string,\n" +
                        "    split_total_amount    string,\n" +
                        "    ts                    bigint,\n" +
                        "    et AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                        "    WATERMARK FOR et AS et - INTERVAL '5' SECOND\n" +
                        ")"+ SQLutil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL)
        );
//        tEnv.executeSql("select * from dwd_trade_order_detail").print();
        //过滤出支付成功表
        readOdsDb(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL);
        Table paySucTable = tEnv.sqlQuery(
                "select\n" +
                        "    `after`['user_id'] user_id,\n" +
                        "    `after`['order_id'] order_id,\n" +
                        "    `after`['payment_type'] payment_type,\n" +
                        "    `after`['callback_time'] callback_time,\n" +
                        "    `pt`,\n" +
                        "    `ts_ms` as ts,\n" +
                        "    `et`\n" +
                        "from ods_initial\n" +
                        "where `source`['table']='payment_info'\n" +
                        "and `op`='u'\n" +
                        "and `before`['payment_status'] is not null \n" +
                        "and `after`['payment_status']='1602'"
        );
        //支付成功表注册为临时表
        tEnv.createTemporaryView("payment_info",paySucTable);
//        tEnv.executeSql("select * from dwd_trade_order_detail").print();

        //读取hbase的base_dic表
        readHbaseDic(tEnv);

        //三表join,其中kafka的payment_info表与dwd_trade_order_detail使用IntervalJoin,
        //筛选条件为订单创建后30min完成支付,可能是要求用户在下单后的 15 分钟内完成支付才有效
        //延迟到支付5秒后,是为了确保在一定时间内，两个系统的数据能够保持同步
        //hbase的表base_dic与kafka映射表ods_initial过滤出的动态表payment_info使用lookup join
        //et为事件时间,为保证结果的准确性和业务语义的一致性。IntervalJoin 中使用et
        //pt为处理时间,也就是数据到达某个算子的系统时间,根据到达时间去hbase中获取对应版本的数据
        //设置水位线event_time - INTERVAL '5' SECOND,意味着窗口允许迟到5秒
        //IntervalJoin 本质上可以看作是在特定时间窗口内的连接操作。
        // 水位线用于确定窗口何时可以触发计算。
        // 当水位线越过窗口的结束时间，Flink 就会触发对该窗口内数据的连接计算
        //LookupJoin 主要用于根据流中的键值去查找维表中的对应数据,所以通常不需要水位线来辅助处理。
        Table joinTable = tEnv.sqlQuery(
                "select\n" +
                        "    od.id order_detail_id,\n" +
                        "    od.order_id,\n" +
                        "    od.user_id,\n" +
                        "    od.sku_id,\n" +
                        "    od.sku_name,\n" +
                        "    od.province_id,\n" +
                        "    od.activity_id,\n" +
                        "    od.activity_rule_id,\n" +
                        "    od.coupon_id,\n" +
                        "    pi.payment_type payment_type_code ,\n" +
                        "    dic.dic_name payment_type_name,\n" +
                        "    pi.callback_time,\n" +
                        "    od.sku_num,\n" +
                        "    od.split_original_amount,\n" +
                        "    od.split_activity_amount,\n" +
                        "    od.split_coupon_amount,\n" +
                        "    od.split_total_amount split_payment_amount,\n" +
                        "    pi.ts \n" +
                        "from payment_info pi\n" +
                        "join dwd_trade_order_detail od\n" +
                        "on pi.order_id=od.order_id\n" +
                        "and od.et>=pi.et-interval '30' minute\n" +
                        "and od.et<=pi.et+interval '5' second\n" +
                        "join base_dic for system_time as of pi.pt as dic\n" +
                        "on pi.payment_type=dic.dic_code;"
        );

        //创建upsert_kafka表
        tEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL+"(\n" +
                        "    order_detail_id string,\n" +
                        "    order_id string,\n" +
                        "    user_id string,\n" +
                        "    sku_id string,\n" +
                        "    sku_name string,\n" +
                        "    province_id string,\n" +
                        "    activity_id string,\n" +
                        "    activity_rule_id string,\n" +
                        "    coupon_id string,\n" +
                        "    payment_type_code string,\n" +
                        "    payment_type_name string,\n" +
                        "    callback_time string,\n" +
                        "    sku_num string,\n" +
                        "    split_original_amount string,\n" +
                        "    split_activity_amount string,\n" +
                        "    split_coupon_amount string,\n" +
                        "    split_payment_amount string,\n" +
                        "    ts bigint,\n" +
                        "    primary key (order_detail_id) not enforced )"
                        +SQLutil.getUpsertKafkaDDl(Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL)
        );

        //写入kafka
        joinTable.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL);

//        tEnv.executeSql("select * from "+Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL).print();



    }
}
