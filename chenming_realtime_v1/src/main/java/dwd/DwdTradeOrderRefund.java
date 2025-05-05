package dwd;


import constant.Constant;
import util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cm.dwd.DwdTradeOrderRefund
 * @Author chen.ming
 * @Date 2025/4/13 19:29
 * @description: 交易域退单事务事实表
 */
public class DwdTradeOrderRefund {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // 1.1 读取 topic_db
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  `before` MAP<string,string>,\n" +
                "  `after` Map<String,String>,\n" +
                "  `source` Map<String,String>,\n" +
                "  `op`  String,\n" +
                "  `ts_ms`  bigint,\n" +
                "  `proc_time` AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'chenming_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        Table table = tenv.sqlQuery("select * from db ");
//        tenv.toChangelogStream(table).print();
// 2. 过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['refund_type'] refund_type," +
                        "after['refund_num'] refund_num," +
                        "after['refund_amount'] refund_amount," +
                        "after['refund_reason_type'] refund_reason_type," +
                        "after['refund_reason_txt'] refund_reason_txt," +
                        "after['create_time'] create_time," +
                        "proc_time," +
                        "ts_ms as ts " +
                        "from db " +
                        "where source['table']='order_refund_info'");
        tenv.createTemporaryView("order_refund_info", orderRefundInfo);
//        tenv.toChangelogStream(orderRefundInfo).print();
//        // 3. 过滤订单表中的退单数据: order_info
        Table orderInfo = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['province_id'] province_id," +
                        "`before` " +
                        "from db " +
                        "where source['table']='order_info' ");
        tenv.createTemporaryView("order_info", orderInfo);
//        tenv.toChangelogStream(orderInfo).print();
        //hbase  ns_xinyi_jiao:dim_base_dic
        tenv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code String,\n" +
                " info ROW<dic_name String>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'ns_chenming:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181'\n" +
                ");");
        Table table1 = tenv.sqlQuery("select * from base_dic ");
//        tenv.toChangelogStream(table1).print();
//        // 4. join: 普通的和 lookup join
        Table result = tenv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(FROM_UNIXTIME(CAST(ri.create_time AS BIGINT) / 1000), 'yyyy-MM-dd') AS date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.proc_time as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.proc_time as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");

                tenv.toChangelogStream(result).print();
//
//        // 5. 写出到 kaf ka
        tenv.executeSql(
                "create table dwd_trade_order_refund (" +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

        result.executeInsert("dwd_trade_order_refund");

        env.execute();
    }
}
