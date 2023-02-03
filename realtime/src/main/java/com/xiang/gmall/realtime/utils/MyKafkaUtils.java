package com.xiang.gmall.realtime.utils;

import com.xiang.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * User: 51728
 * Date: 2022/10/28
 * Desc: 操作kafka工具类
 */
public class MyKafkaUtils {
    private static final String DEFAULT_TOPIC="default_topic";
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092,hadoop102:9092,hadoop103:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                props
        );
    }
// 不能保证精准一致性
//    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
//
//        return new FlinkKafkaProducer<String>(
//                "hadoop101:9092,hadoop102:9092,hadoop103:9092",
//                topic,
//                new SimpleStringSchema()
//        );
//    }
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092,hadoop102:9092,hadoop103:9092");
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,1000*60*15+"");
        return new FlinkKafkaProducer<String>(DEFAULT_TOPIC,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                        return new ProducerRecord<byte[], byte[]>(topic,s.getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092,hadoop102:9092,hadoop103:9092");
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,1000*60*15+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    // 给Flink SQL使用的
    public static String getKafkaDDL(String topic,String groupId){
        String ddl = "'connector' = 'kafka'," +
                " 'topic' = '"+ topic +"'," +
                " 'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092'," +
                " 'properties.group.id' = '"+  groupId +"'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'format' = 'json'";
        return ddl;
    }

    public static void main(String[] args) {
        System.out.println(getKafkaDDL("a","group"));
    }
}
