package com.xiang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.app.dwm.func.DimAsyncFunction;
import com.xiang.gmall.realtime.beans.OrderDetail;
import com.xiang.gmall.realtime.beans.OrderInfo;
import com.xiang.gmall.realtime.beans.OrderWide;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * User: 51728
 * Date: 2022/11/13
 * Desc:
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmall"));
        System.setProperty("HADOOP_USER_NAME","xiang");

        String orderInfoTopic = "dwd_order_info";
        String orderDetailTopic = "dwd_order_detail";
        String groupId = "dwd_order_wide_app";
        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtils.getKafkaSource(orderInfoTopic, groupId);
        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtils.getKafkaSource(orderDetailTopic, groupId);
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(orderInfoKafkaSource);
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(orderDetailKafkaSource);

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoKafkaDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String s) throws Exception {
                        OrderInfo orderInfo = JSONObject.parseObject(s, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailKafkaDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String s) throws Exception {
                        OrderDetail orderDetail = JSONObject.parseObject(s, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

        // TODO 5.分配水位线
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );

        // TODO 6.分组
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithWatermarkDS.keyBy(new KeySelector<OrderInfo, Long>() {
            @Override
            public Long getKey(OrderInfo orderInfo) throws Exception {
                return orderInfo.getId();
            }
        });
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithWatermarkDS.keyBy(new KeySelector<OrderDetail, Long>() {
            @Override
            public Long getKey(OrderDetail orderDetail) throws Exception {
                return orderDetail.getOrder_id();
            }
        });

        // TODO 7.双流join 使用interval join 一般是一对多join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS.intervalJoin(orderDetailKeyedDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        // orderWideDS.print("wide$$$");
        // TODO 8.和用户维度进行关联

        /* unorderedWait方法就是说同时多个数据和Hbase数据进行信息的请求，
         返回结果的时候我们不需要对这多个数据进行原始的顺序排序，来一个处理一个就好
         这里是考虑到维度数据与信息无关，如果是有关顺序的使用可以使用orderedWait方法
         */
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfo = AsyncDataStream.unorderedWait(
                orderWideDS, // 流
                // 处理DIM_USER_INFO的维度关联操作
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                    @Override
                    public void join(OrderWide obj, JSONObject jsonObject) throws Exception {
                        // 获取dim_user_info表格中的gender和年龄
                        String gender = jsonObject.getString("GENDER");
                        String birthday = jsonObject.getString("BIRTHDAY");
                        Date date = sdf.parse(birthday);

                        Long nowTimeTs = System.currentTimeMillis();
                        Long betweenTs = nowTimeTs - date.getTime();
                        Long ageLong = betweenTs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();

                        obj.setUser_age(age);
                        obj.setUser_gender(gender);

                    }

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getUser_id().toString();
                    }
                }, // 实现异步关联的逻辑
                60, // 数据返回超时时间
                TimeUnit.SECONDS // 单位
        );

        // TODO 9.和地区维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithUserProDS = AsyncDataStream.unorderedWait(
                orderWideWithUserInfo,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide obj, JSONObject jsonObject) throws Exception {
                        obj.setProvince_name(jsonObject.getString("NAME"));
                        obj.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        obj.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                        obj.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                    }

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getProvince_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // TODO 10. 和SKU维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithUserProDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide obj, JSONObject jsonObject) throws Exception {
                        obj.setSku_name(jsonObject.getString("SKU_NAME"));
                        obj.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        obj.setSpu_id(jsonObject.getLong("SPU_ID"));
                        obj.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getSku_id().toString();
                    }
                },
                60, TimeUnit.SECONDS
        );
        // TODO 11. 和SPU维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);


        // TODO 12. 和类别维度进行关联

        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);


        // TODO 13. 品牌维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // TODO 14. 写回kafka数据
        orderWideWithTmDS
                .map(new MapFunction<OrderWide, String>() {
                    @Override
                    public String map(OrderWide orderWide) throws Exception {
                        return JSONObject.toJSONString(orderWide);
                    }
                })
                .addSink(
                MyKafkaUtils.getKafkaSink("dwm_order_wide")
        );
        env.execute();
    }
}
