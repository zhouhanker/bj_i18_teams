package com.zsf.retail_v1.realtime.util;

import com.alibaba.fastjson.JSONObject;

import com.zsf.retail_v1.realtime.bean.TableProcessDwd;
import com.zsf.retail_v1.realtime.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author Felix
 * @date 2024/5/29
 * 获取相关Source的工具类
 */
public class FlinkSinkUtil {
    //获取KafkaSink
    public static KafkaSink<String> getKafkaSink(String topic){
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //设置事务Id的前缀
                //.setTransactionalIdPrefix("dwd_base_log_")
                //设置事务的超时时间   检查点超时时间 <     事务的超时时间 <=事务最大超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
        return kafkaSink;
    }

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink(){
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2, KafkaSinkContext context, Long timestamp) {
                        JSONObject jsonObj = tup2.f0;
                        TableProcessDwd tableProcessDwd = tup2.f1;
                        String topic = tableProcessDwd.getSinkTable();
                        return new ProducerRecord<byte[], byte[]>(topic, jsonObj.toJSONString().getBytes());
                    }
                })
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //设置事务Id的前缀
                //.setTransactionalIdPrefix("dwd_base_log_")
                //设置事务的超时时间   检查点超时时间 <     事务的超时时间 <=事务最大超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
        return kafkaSink;
    }
    //扩展：如果流中数据类型不确定，如果将数据写到kafka主题
    public static <T>KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> ksr){
        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(ksr)
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //设置事务Id的前缀
                //.setTransactionalIdPrefix("dwd_base_log_")
                //设置事务的超时时间   检查点超时时间 <     事务的超时时间 <=事务最大超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
        return kafkaSink;
    }
}
