package com.xiang.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.xiang.gmall.realtime.app.dwm.func.DimAsyncFunction;
import com.xiang.gmall.realtime.beans.OrderWide;
import com.xiang.gmall.realtime.beans.PaymentWide;
import com.xiang.gmall.realtime.beans.ProductStats;
import com.xiang.gmall.realtime.common.GmallConstant;
import com.xiang.gmall.realtime.utils.ClickHouseUtil;
import com.xiang.gmall.realtime.utils.DateTimeUtil;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Product;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * User: 51728
 * Date: 2022/11/15
 * Desc: 需要启动的：基本全部都需要启动
 *  hadoop、zookeeper、Kafka、logger、
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception{
        // TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2.检查点准备
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmall"));
        System.setProperty("HADOOP_USER_NAME","xiang");

        // TODO 3.读取Kafka数据
        String dwdPageLogTopic = "dwd_page_log";
        //String dwdDisplayLogTopic = "dwd_display_log";
        String dwd_favor_infoTopic = "dwd_favor_info";
        String dwd_cart_infoTopic = "dwd_cart_info";
        String dwm_order_wideTopic = "dwm_order_wide";
        String dwm_payment_wideTopic = "dwm_payment_wide";
        String dwd_order_refund_infoTopic = "dwd_order_refund_info";
        String dwd_comment_infoTopic = "dwd_comment_info";
        String groupId = "dws_ProductStats_app";

        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtils.getKafkaSource(dwdPageLogTopic, groupId));
        //DataStreamSource<String> displayLogDS = env.addSource(MyKafkaUtils.getKafkaSource(dwdDisplayLogTopic, groupId));
        DataStreamSource<String> favorInfoDS = env.addSource(MyKafkaUtils.getKafkaSource(dwd_favor_infoTopic, groupId));
        DataStreamSource<String> cartInfoDS = env.addSource(MyKafkaUtils.getKafkaSource(dwd_cart_infoTopic, groupId));
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtils.getKafkaSource(dwm_order_wideTopic, groupId));
        DataStreamSource<String> paymentWideDS = env.addSource(MyKafkaUtils.getKafkaSource(dwm_payment_wideTopic, groupId));
        DataStreamSource<String> orderRefundInfoDS = env.addSource(MyKafkaUtils.getKafkaSource(dwd_order_refund_infoTopic, groupId));
        DataStreamSource<String> commentInfoDS = env.addSource(MyKafkaUtils.getKafkaSource(dwd_comment_infoTopic, groupId));

//        pageLogDS.print("page");
//        favorInfoDS.print("favor@@");
//        cartInfoDS.print("cart");
//        orderWideDS.print("orderWide");
//        paymentWideDS.print("payment");
//        orderRefundInfoDS.print("refund");
//        commentInfoDS.print("comment");

        // TODO 4. 对流中的数据进行类型的转换 jsonStr->ProductStats

        // 点击和曝光流的处理
        // 测试无问题
        SingleOutputStreamOperator<ProductStats> clickAndDisplayObjDS = pageLogDS.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, ProductStats>.Context context, Collector<ProductStats> collector) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        //System.out.println("page" + jsonObject);
                        Long ts = jsonObject.getLong("ts");
                        // 判断是否为点击行为
                        JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                        String page_id = pageJsonObj.getString("page_id");
                        if (page_id.equals("good_detail")) {
                            Long skuID = pageJsonObj.getLong("item");
                            ProductStats productStats = ProductStats.builder()
                                    .click_ct(1L)
                                    .sku_id(skuID)
                                    .ts(ts)
                                    .build();
                            collector.collect(productStats);
                        }
                        // 判断是否为曝光日志
                        JSONArray displays = jsonObject.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject displayJSONObject = displays.getJSONObject(i);
                                if(displayJSONObject == null){
                                    System.out.println("出现了空的情况");
                                    System.out.println("dis@@"+displays);
                                }
                                // 判断曝光的是不是商品
                                if (displayJSONObject != null && displayJSONObject.getString("item_type").equals("sku_id")) {
                                    Long disSkuId = displayJSONObject.getLong("item");
                                    ProductStats build = ProductStats.builder()
                                            .sku_id(disSkuId)
                                            .display_ct(1L)
                                            .ts(ts)
                                            .build();
                                    collector.collect(build);
                                }

                            }
                        }
                    }
                }
        );
        // 收藏流的处理
        // 测试无问题
        SingleOutputStreamOperator<ProductStats> favorObjDS = favorInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String s) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        System.out.println("favor@@" + jsonObject);
                        ProductStats build = ProductStats.builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(DateTimeUtil.getTimeTs(jsonObject.getString("create_time")))
                                .build();

                        return build;
                    }
                }
        );
        // 转换加购流的处理
        // 测试无问题
        SingleOutputStreamOperator<ProductStats> cartObjDS = cartInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String s) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        //System.out.println("cart:" + jsonObject);
                        ProductStats build = ProductStats.builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .cart_ct(1L)
                                .ts(DateTimeUtil.getTimeTs(jsonObject.getString("create_time")))
                                .build();

                        return build;
                    }
                }
        );
        // 退款流的处理
        // 测试无问题
        SingleOutputStreamOperator<ProductStats> refundOrderObjDS = orderRefundInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String s) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        //System.out.println("refund:" + jsonObject);
                        HashSet hashSet = new HashSet();
                        hashSet.add(jsonObject.getLong("order_id"));
                        ProductStats build = ProductStats.builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .refundOrderIdSet(hashSet)
                                .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                                .ts(DateTimeUtil.getTimeTs(jsonObject.getString("create_time")))
                                .build();

                        return build;
                    }
                }
        );
        // 测试无问题
        SingleOutputStreamOperator<ProductStats> commentInfoObjDS = commentInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String s) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        //System.out.println("comment:" + jsonObject);
                        String appraise = jsonObject.getString("appraise");
                        Long goodNumber = 0L;
                        if(appraise.equals(GmallConstant.APPRAISE_GOOD)){
                            goodNumber = 1L;
                        }

                        ProductStats build = ProductStats.builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .comment_ct(1L)
                                .good_comment_ct(goodNumber)
                                .ts(DateTimeUtil.getTimeTs(jsonObject.getString("create_time")))
                                .build();

                        return build;
                    }
                }
        );
        //测试无问题
        SingleOutputStreamOperator<ProductStats> orderWideObjDS = orderWideDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String s) throws Exception {
                        OrderWide orderWide = new Gson().fromJson(s, OrderWide.class);
                        //System.out.println("orderWide"+orderWide);
                        Long order_id = orderWide.getOrder_id();
                        HashSet hash = new HashSet();
                        hash.add(order_id);
                        return ProductStats.builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getSplit_total_amount())
                                .ts(DateTimeUtil.getTimeTs(orderWide.getCreate_time()))
                                .orderIdSet(hash)
                                .build();
                    }
                }
        );
        // 测试无问题
        SingleOutputStreamOperator<ProductStats> paymentWideObjDS = paymentWideDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String s) throws Exception {
                        PaymentWide paymentWide = new Gson().fromJson(s, PaymentWide.class);
                        //System.out.println("paymentWide"+paymentWide);
                        return ProductStats.builder()
                                .sku_id(paymentWide.getSku_id())
                                .payment_amount(paymentWide.getSplit_total_amount())
                                .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                                .ts(DateTimeUtil.getTimeTs(paymentWide.getCallback_time()))
                                .build();
                    }
                }
        );
        // TODO 5.合并流
        DataStream<ProductStats> unionDS = clickAndDisplayObjDS.union(
                favorObjDS,
                cartObjDS,
                refundOrderObjDS,
                commentInfoObjDS,
                orderWideObjDS,
                paymentWideObjDS
        );
        //unionDS.print("&&&&");
        // TODO 6.指定水位线
        SingleOutputStreamOperator<ProductStats> withWaterMarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long l) {
                        return productStats.getTs();
                    }
                }));

        // TODO 7.分组 目前是商品维度数据处理，这里使用skuId来进行分组
        KeyedStream<ProductStats, Long> keyedDS = withWaterMarkDS.keyBy(new KeySelector<ProductStats, Long>() {
            @Override
            public Long getKey(ProductStats productStats) throws Exception {
                return productStats.getSku_id();
            }
        });

        // TODO 8.开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // TODO 9.聚合计算
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;

                    }
                }
                , new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>.Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        for (ProductStats productStats : elements) {
                            productStats.setStt(DateTimeUtil.getTimeDateStr(new Date(context.window().getStart())));
                            productStats.setEdt(DateTimeUtil.getTimeDateStr(new Date(context.window().getEnd())));
                            productStats.setTs(new Date().getTime());
                            out.collect(productStats);
                        }
                    }
                }
        );
        // TODO 10.补充商品维度数据
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public void join(ProductStats obj, JSONObject jsonObject) throws Exception {
                        // 关联商品的维度信息
                        obj.setSku_name(jsonObject.getString("SKU_NAME"));
                        obj.setSku_price(jsonObject.getBigDecimal("PRICE"));
                        obj.setSpu_id(jsonObject.getLong("SPU_ID"));
                        obj.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        obj.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getSku_id().toString();
                    }
                }, 60, TimeUnit.SECONDS
        );
        // 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        // 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        // 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        productStatsWithTmDS.print("result##");

        // TODO 11.将结果写到ClickHouse
        productStatsWithTmDS.addSink(
                ClickHouseUtil.getSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );
        env.execute();
    }
}
