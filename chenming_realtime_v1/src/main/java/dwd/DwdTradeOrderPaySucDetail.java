package dwd;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cm.dwd.DwdTradeOrderPaySucDetail
 * @Author chen.ming
 * @Date 2025/4/13 18:42
 * @description: 交易域支付成功事务事实表
 */
public class DwdTradeOrderPaySucDetail {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //TODO 从下单事实表读取数据创建动态表
        tenv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
                "id STRING,\n" +
                "order_id STRING,\n" +
                "user_id STRING,\n" +
                "sku_id STRING,\n" +
                "sku_name STRING,\n" +
                "province_id STRING,\n" +
                "activity_id STRING,\n" +
                "activity_rule_id STRING,\n" +
                "coupon_id STRING,\n" +
                "date_id STRING,\n" +
                // 修改为 BIGINT 类型处理时间戳
                "create_time BIGINT,\n" +
                "sku_num STRING,\n" +
                "split_original_amount STRING,\n" +
                "split_activity_amount STRING,\n" +
                "split_coupon_amount STRING,\n" +
                "split_total_amount STRING,\n" +
                "ts BIGINT,\n" +
                // 使用毫秒级时间戳转换
                "et AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "WATERMARK FOR et AS et - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_trade_order_detail_chenming',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                // 添加 JSON 解析容错配置
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")");
        Table table = tenv.sqlQuery("select * from dwd_trade_order_detail ");
//        tenv.toChangelogStream(table).print();
        //TODO 从topic_db主题中读取数据创建动态表
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  before MAP<string,string>,\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  proc_time  AS proctime(),\n "+
                "  et AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" +
                "  WATERMARK FOR et AS et - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'chenming_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        Table table1 = tenv.sqlQuery("select * from db ");
//        tenv.toChangelogStream(table1).print();
        //TODO 过滤出支付成功数据
        Table paymentInfo = tenv.sqlQuery("select " +
                "after['user_id'] user_id," +
                "after['order_id'] order_id," +
                "after['payment_type'] payment_type," +
                "after['callback_time'] callback_time," +
                "`proc_time`," +
                "ts_ms as ts, " +
                "et " +
                "from db " +
                "where source['table']='payment_info' ");
        tenv.createTemporaryView("payment_info", paymentInfo);
//        tenv.toChangelogStream(paymentInfo).print(" 过滤出支付成功数据");
        //TODO 从HBase中读取字典数据创建动态表
        tenv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code String,\n" +
                " info ROW<dic_name String>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'ns_chenming:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181'\n" +
                ");");
        Table table2 = tenv.sqlQuery("select * from db ");
//        tenv.toChangelogStream(table2).print();
        //TODO 支付和字典进行关联---lookupjoin
        Table result = tenv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.proc_time as dic " +
                        "on pi.payment_type=dic.dic_code ");
        tenv.toChangelogStream(result).print();

        //TODO 将关联的结果写到kafka主题中
        tenv.executeSql("create table dwd_trade_order_payment_success(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint ," +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED " +
                ") " +
                " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_trade_order_payment_success_chenming',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        result.executeInsert("dwd_trade_order_payment_success");

        env.execute();
    }
}
