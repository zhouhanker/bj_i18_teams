package dwd;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import constant.Constant;
import utils.SQLUtil;

import java.time.Duration;

/**
 * @Package dwd.DwdTradeOrderRefund
 * @Author yinshi
 * @Date 2025/5/4 15:26
 * @description: DwdTradeOrderRefund
 */

public class DwdTradeOrderRefund {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        tableEnv.executeSql("select * from topic_db").print();


        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
//        tableEnv.executeSql("select * from base_dic").print();

        // 2. 过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +
                        " after['user_id'] user_id," +
                        " after['order_id'] order_id," +
                        " after['sku_id'] sku_id," +
                        " after['refund_type'] refund_type," +
                        " after['refund_num'] refund_num," +
                        " after['refund_amount'] refund_amount," +
                        " after['refund_reason_type'] refund_reason_type," +
                        " after['refund_reason_txt'] refund_reason_txt," +
                        " after['create_time'] create_time," +
                        " ts_ms " +
                        " from topic_db " +
                        " where source['table'] = 'order_refund_info' " +
                        " and `op`='r' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
//        orderRefundInfo.execute().print();

        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        " after['id'] id," +
                        " after['province_id'] province_id " +
                        " from topic_db " +
                        " where source['table'] = 'order_info' " +
                        " and `op`='r' " +
                        " and `after`['order_status'] is not null " +
                        " and `after`['order_status'] = '1006' ");
        tableEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();

        // 4. join: 普通的和 lookup join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "  ri.id," +
                        "  ri.user_id," +
                        "  ri.order_id," +
                        "  ri.sku_id," +
                        "  oi.province_id," +
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(ri.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        "  ri.create_time," +
                        "  ri.refund_type," +
                        "  ri.refund_reason_type," +
                        "  ri.refund_reason_txt," +
                        "  ri.refund_num," +
                        "  ri.refund_amount," +
                        "  ri.ts_ms " +
                        "  from order_refund_info ri " +
                        "  join order_info oi " +
                        "  on ri.order_id=oi.id ");
//        result.execute().print();

        // 5. 写出到 kafka
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_reason_type_code string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts_ms bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

        env.execute("DwdTradeOrderRefund");
    }
}
