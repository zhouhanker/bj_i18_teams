package com.atguigu.gmall.realtime.common.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

public class MyKafkaUtil {
    private static final String brokers = "cdh01:9092,cdh02:9092,cdh03:9092";
    private static final String default_topic = "DWD_DEFAULT_TOPIC";//一般用不上，随便取名
    //生产者方法
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(brokers,topic,new SimpleStringSchema());
    }


    //消费者方法
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic){
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"groupId"+ new Random().nextInt(10));
        //earliest
        /*earliest
        当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        latest
        当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        none
        topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常*/
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);

        return new FlinkKafkaProducer<T>(default_topic,
                kafkaSerializationSchema,
                properties,
                //如果没有开启checkpoint，这块设置为EXACTLY_ONCE没有用
                //大家感兴趣可以将CK打开，下面设置为EXACTLY_ONCE，用这种会报一个错，自己去尝试找这个错
                FlinkKafkaProducer.Semantic.NONE);
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return  " with ( 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset' ) ";
    }
}
