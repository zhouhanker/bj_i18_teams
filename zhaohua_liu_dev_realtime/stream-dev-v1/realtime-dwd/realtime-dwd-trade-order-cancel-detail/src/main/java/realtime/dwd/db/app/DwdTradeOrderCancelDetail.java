package realtime.dwd.db.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.common.base.BaseSQLApp;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

import java.time.Duration;

/**
 * @Package realtime.dwd.db.app.DwdTradeOrderCancelDetail
 * @Author zhaohua.liu
 * @Date 2025/4/16.11:37
 * @description: 退单事实表
 */
public class DwdTradeOrderCancelDetail extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeOrderCancelDetail().start(20007,4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL);
    }
    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //设置状态保留时间
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        //从kafka读取ods数据
        readOdsDb(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL);
        //过滤出取消行为
        Table OrderCancelTable = tEnv.sqlQuery(
                "select\n" +
                        "    `after`['id'] id,\n" +
                        "    `after`['operate_time'] operate_time,\n" +
                        "    `ts_ms`\n" +
                        "from ods_initial\n" +
                        "where `source`['table']='order_info'\n" +
                        "and `op`='u'\n" +
                        "and `before`['order_status']='1001'\n" +
                        "and `after`['order_status']='1003'"
        );
//        OrderCancelTable.execute().print();
        //注册为临时表
        tEnv.createTemporaryView("order_cancel_detail",OrderCancelTable);
        //获取订单明细表
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
                        "ts bigint)"+ SQLutil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL)
        );
        //将下单事实表和取消订单表进行关联
        Table JoinTable = tEnv.sqlQuery(
                "select\n" +
                        "    od.id ,\n" +
                        "    od.order_id , \n" +
                        "    od.user_id , \n" +
                        "    od.sku_id , \n" +
                        "    od.sku_name , \n" +
                        "    od.province_id , \n" +
                        "    od.activity_id , \n" +
                        "    od.activity_rule_id , \n" +
                        "    od.coupon_id , \n" +
                        "    date_format(TO_TIMESTAMP_LTZ(cast(oc.operate_time as bigint),3),cast('yyyy-MM-dd' as string)) order_cancel_date_id,\n" +
                        "    oc.operate_time,\n" +
                        "    od.sku_num ,\n" +
                        "    od.split_original_amount , \n" +
                        "    od.split_activity_amount , \n" +
                        "    od.split_coupon_amount , \n" +
                        "    od.split_total_amount , \n" +
                        "    oc.ts_ms ts \n" +
                        "from dwd_trade_order_detail od\n" +
                        "join order_cancel_detail oc on oc.id=od.order_id"
        );

        //upsert_kafka映射表
        tEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL+"(" +
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
                        ")" + SQLutil.getUpsertKafkaDDl(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL)
        );

        JoinTable.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL);



    }
}
