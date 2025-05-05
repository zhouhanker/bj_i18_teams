package com.rb.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.DwdUtils;
import com.rb.utils.SQLUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @Package com.rb.dwd.DwdTradeOrderDetail
 * @Author runbo.zhang
 * @Date 2025/4/10 22:09
 * @description:
 */
public class DwdTradeOrderDetail {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
//        System.setProperty("HADOOP_USER_NAME", "hdfs");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DwdUtils.dwdKafkaDbInit(tEnv, "log_topic_flink_online_v1_dwd");

        DwdUtils.hbaseBaseDicInit(tEnv,"dim_zrb_online_v1", "dim_base_dic");

        //订单详情表
        Table orderDetail = tEnv.sqlQuery("select  `after`['id'] id " +
                "\n, `after`['order_id'] order_id " +
                "\n, `after`['sku_id']  sku_id" +
                "\n, `after`['sku_name']  sku_name" +
                "\n, `after`['create_time']  create_time" +
                "\n, `after`['source_id']  source_id" +
                "\n, `after`['source_type']  source_type" +
                "\n, `after`['sku_num']  sku_num" +
                "\n, cast(cast(`after`['sku_num'] as decimal(16,2)) * " +
                "  cast(`after`['order_price'] as decimal(16,2)) as String) as split_original_amount" +
                "\n, `after`['split_total_amount']  split_total_amount" +
                "\n, `after`['split_activity_amount']  split_activity_amount" +
                "\n, `after`['split_coupon_amount']  split_coupon_amount" +
                "\n, `source`['ts_ms']  ts_ms" +
                "\n from topic_db" +
                " where `source`['table']='order_detail' " +
                "   and op='c'");
        tEnv.createTemporaryView("order_detail", orderDetail);

//        orderDetail.execute().print();
        //订单表
        Table orderInfo = tEnv.sqlQuery("select  `after`['id'] id " +
                "\n, `after`['user_id'] user_id " +
                "\n, `after`['province_id']  province_id" +
                "\n from topic_db" +
                " where `source`['table']='order_info' " +
                "   and op='c'");
        tEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();


        //活动表
        Table orderDetailActivity = tEnv.sqlQuery("select  `after`['id'] id " +
                "\n, `after`['order_detail_id'] order_detail_id " +
                "\n, `after`['activity_id']  activity_id" +
                "\n, `after`['activity_rule_id']  activity_rule_id" +
                "\n from topic_db" +
                " where `source`['table']='order_detail_activity' " +
                "   and op='c'");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
//        orderDetailActivity.execute().print();

        //优惠券表
        Table orderDetailCoupon = tEnv.sqlQuery("select  `after`['id'] id" +
                "\n, `after`['order_detail_id'] order_detail_id " +
                "\n, `after`['coupon_id']  coupon_id" +
                "\n from topic_db" +
                " where `source`['table']='order_detail_coupon' " +
                "   and op='c'");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
//        orderDetailCoupon.execute().print();


        Table result = tEnv.sqlQuery(
                "select " +
                        " od.id," +
                        " od.order_id," +
                        " oi.user_id," +
                        " od.sku_id," +
                        " od.sku_name," +
                        " oi.province_id," +
                        " act.activity_id," +
                        " act.activity_rule_id," +
                        " cou.coupon_id," +
                        " date_format(FROM_UNIXTIME(CAST(od.create_time AS bigint)/1000), 'yyyy-MM-dd') date_id," +  // 年月日
                        " od.create_time," +
                        " od.sku_num," +
                        " od.split_original_amount," +
                        " od.split_activity_amount," +
                        " od.split_coupon_amount," +
                        " od.split_total_amount," +
                        " od.ts_ms " +
                        " from order_detail od " +
                        " inner join order_info oi on od.order_id=oi.id and od.id is not null" +
                        " left join order_detail_activity act " +
                        " on od.id=act.order_detail_id and act.order_detail_id is not null " +
                        " left join order_detail_coupon cou " +
                        " on od.id=cou.order_detail_id and cou.order_detail_id is not null " );
//        tEnv.createTemporaryView("", );
//         tEnv.toDataStream(result).print();

        List<String> names = result.getResolvedSchema().getColumnNames();

        SingleOutputStreamOperator<String> jsonDs = tEnv.toChangelogStream(result)
                .map(new MapFunction<Row, String>() {
                    @Override
                    public String map(Row row) throws Exception {

                        JSONObject object = new JSONObject();
                        for (int i = 0; i < row.getArity(); i++) {
                            object.put(names.get(i), row.getField(i));
                        }
                        return object.toJSONString();

                    }
                });

        jsonDs.sinkTo(SourceSinkUtils.sinkToKafka("dwd_trade_order_detail_v1"));
        env.execute();
//        result.execute().print();
//


        tEnv.executeSql(
                "create table "+"dwd_trade_order_detail_v1"+"(" +
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
                        "ts_ms string," +
                        "primary key(id) not enforced " +
                        ")" + SQLUtil.getUpsertKafkaDDL("dwd_trade_order_detail_v1"));

//        result.executeInsert("dwd_trade_order_detail_v1");
//        result.e



//        env.execute("ljklkjk");
    }


}
