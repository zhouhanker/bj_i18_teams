package com.hwq.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.hwq
 * @Author hu.wen.qi
 * @Date 2025/04/30 11:29
 * @description: 1
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        //设置微妙和把decmial换成double
        properties.setProperty("decimal.handling.mode","double");
        properties.setProperty("time.precision.mode","connect");
        //flinkcdc采集
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("dev_realtime_v6_wenqi_hu") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("dev_realtime_v6_wenqi_hu.*") // 设置捕获的表
                .username("root")
                .password("root")
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.initial()) // 从最早位点启动
                //.startupOptions(StartupOptions.latest()) // 从最晚位点启动
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> ds1 = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");


        //ds1.print();

        //{"before":null,"after":{"id":3447,"order_id":2134,"sku_id":5,"sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米","img_url":null,"order_price":999.0,"sku_num":1,"create_time":1744049874000,"split_total_amount":999.0,"split_activity_amount":0.0,
        // "split_coupon_amount":0.0,"operate_time":null},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744076986000,"snapshot":"false","db":"dev_realtime_v3_wenqi_hu","sequence":null,
        // "table":"order_detail","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":3825924,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1744091723610,"transaction":null}
        //ds1.print();

        //判断是否是json格式
        SingleOutputStreamOperator<String> kafka = ds1.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) {
                boolean b = JSON.isValid(s);
                if (!b){
                    return false;
                }
                JSONObject all = JSON.parseObject(s);
                JSONObject after = all.getJSONObject("after");
                return after != null;
            }
        });
        //{"before":null,"after":{"id":3447,"order_id":2134,"sku_id":5,"sku_name":"Redmi 10X 4G Helio G85游戏芯
        // 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米","img_url":null,"order_price":999.0,"sku_num":1,
        // "create_time":1744049874000,"split_total_amount":999.0,"split_activity_amount":0.0,"split_coupon_amount":0.0,"operate_time":null},
        // "source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744076986000,"snapshot":"false",
        // "db":"dev_realtime_v3_wenqi_hu","sequence":null,"table":"order_detail","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":3825924,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1744091723610,"transaction":null}
         //ds2.print();

       kafka.print();
      // kafka.addSink(KafkaUtil.getKafkaSink("log_topic"));


        //{"before":{"id":2239,"consignee":"窦裕河","consignee_tel":"13248136725","total_amount":13659.0,"order_status":"1001","user_id":122,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"411646696444523","trade_body":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g等3件商品","create_time":1744068542000,"operate_time":null,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":24,"activity_reduce_amount":250.0,"coupon_reduce_amount":0.0,"original_total_amount":13909.0,"feight_fee":null,"feight_fee_reduce":null,"refundable_time":1744673342000},"after":{"id":2239,"consignee":"窦裕河","consignee_tel":"13248136725","total_amount":13659.0,"order_status":"1002","user_id":122,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"411646696444523","trade_body":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g等3件商品","create_time":1744068542000,"operate_time":1744068570000,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":24,"activity_reduce_amount":250.0,"coupon_reduce_amount":0.0,"original_total_amount":13909.0,"feight_fee":null,"feight_fee_reduce":null,"refundable_time":1744673342000},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744076990000,"snapshot":"false","db":"dev_realtime_v3_wenqi_hu","sequence":null,"table":"order_info","server_id":1,
        // "gtid":null,"file":"mysql-bin.000002","pos":4511434,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1744091723636,"transaction":null}
        //kafka读


        env.execute("Print MySQL Snapshot + Binlog");
    }
}
