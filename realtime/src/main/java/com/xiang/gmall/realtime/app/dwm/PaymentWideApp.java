package com.xiang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.xiang.gmall.realtime.beans.OrderWide;
import com.xiang.gmall.realtime.beans.PaymentInfo;
import com.xiang.gmall.realtime.beans.PaymentWide;
import com.xiang.gmall.realtime.utils.DateTimeUtil;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * User: 51728
 * Date: 2022/11/14
 * Desc:
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception{
        // TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // TODO 2.检查点设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmall"));
        System.setProperty("HADOOP_USER_NAME","xiang");
        // TODO 3.从Kafka中读取数据
        String paymentInfoTopic = "dwd_payment_info";
        String orderWideTopic = "dwm_order_wide";
        String groupID = "payment_wide_app_group";

        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtils.getKafkaSource(paymentInfoTopic, groupID);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtils.getKafkaSource(orderWideTopic, groupID);

        DataStreamSource<String> paymentInfoDS = env.addSource(paymentInfoSource);
        DataStreamSource<String> orderWideDS = env.addSource(orderWideSource);

        // TODO 4.对数据格式进行转换  str->class
        SingleOutputStreamOperator<PaymentInfo> paymentInfoObjDS = paymentInfoDS.map(new MapFunction<String, PaymentInfo>() {
            @Override
            public PaymentInfo map(String s) throws Exception {
                return JSONObject.parseObject(s, PaymentInfo.class);
            }
        });
        SingleOutputStreamOperator<OrderWide> orderWideObjDS = orderWideDS.map(new MapFunction<String, OrderWide>() {
            @Override
            public OrderWide map(String s) throws Exception {
                return new Gson().fromJson(s, OrderWide.class);
            }
        });

        // TODO 5.指定水位线
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                        return DateTimeUtil.getTimeTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long l) {
                                        return DateTimeUtil.getTimeTs(orderWide.getCreate_time());
                                    }
                                }
                        )
        );
        
        // TODO 6.按照订单id分组，为了指定关联字段
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(
                new KeySelector<PaymentInfo, Long>() {
                    @Override
                    public Long getKey(PaymentInfo paymentInfo) throws Exception {
                        return paymentInfo.getOrder_id();
                    }
                }
        );
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(
                new KeySelector<OrderWide, Long>() {
                    @Override
                    public Long getKey(OrderWide orderWide) throws Exception {
                        return orderWide.getOrder_id();
                    }
                }
        );

        // TODO 7.进行双流join

        SingleOutputStreamOperator<PaymentWide> payWideDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context context, Collector<PaymentWide> collector) throws Exception {
                                collector.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );
        payWideDS.print("=====");
        // TODO 8.把支付款表数据写入Kafka
        payWideDS
                .map(new MapFunction<PaymentWide, String>() {
                    @Override
                    public String map(PaymentWide paymentWide) throws Exception {
                        return JSONObject.toJSONString(paymentWide);
                    }
                })
                .addSink(MyKafkaUtils.getKafkaSink("dwm_payment_wide"))
        ;
        env.execute();
    }
}
