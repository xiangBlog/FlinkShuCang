package com.xiang.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.beans.VisitorStats;
import com.xiang.gmall.realtime.utils.ClickHouseUtil;
import com.xiang.gmall.realtime.utils.DateTimeUtil;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * User: 51728
 * Date: 2022/11/15
 * Desc: 测试
 *  需要启动hadoop zookeeper kafka 采集服务
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception{
        // TODO 1.基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2.检查点操作
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmall"));
        System.setProperty("HADOOP_USER_NAME","xiang");

        // TODO 3.从kafka中读取数据
        String dwdPageLogTopic = "dwd_page_log";
        String dwmUniqueVisitorTopic = "dwm_unique_visitor";
        String dwmJumpTopic = "dwm_user_jump_detail";
        String groupId = "dws_visitor_stats_app";
        DataStreamSource<String> dwdPageLogDS = env.addSource(MyKafkaUtils.getKafkaSource(dwdPageLogTopic, groupId));
        DataStreamSource<String> dwmUniqueVisitorDS = env.addSource(MyKafkaUtils.getKafkaSource(dwmUniqueVisitorTopic, groupId));
        DataStreamSource<String> dwmJumpDS = env.addSource(MyKafkaUtils.getKafkaSource(dwmJumpTopic, groupId));

        // TODO 4.把三条流转换为实体类
        // 从dwd_page_id做转换
        SingleOutputStreamOperator<VisitorStats> pvObjDS = dwdPageLogDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        pageJsonObj.getLong("during_time"),
                        jsonObject.getLong("ts")
                );
                String last_page_id = pageJsonObj.getString("last_page_id");
                if (last_page_id == null || last_page_id.length() == 0) {
                    visitorStats.setSv_ct(1L);
                }
                return visitorStats;
            }
        });
        // 从dwm_un 做转换
        SingleOutputStreamOperator<VisitorStats> uvObjDS = dwmUniqueVisitorDS.map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                        return visitorStats;
                    }
                });
        // 从dwm_jump做转换
        SingleOutputStreamOperator<VisitorStats> jumpObjDS = dwmJumpDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObject.getLong("ts")
                );
                return visitorStats;
            }
        });

        // TODO 5.把三条流进行合并
        DataStream<VisitorStats> unionDS = pvObjDS.union(uvObjDS, jumpObjDS);

        // TODO 6.合并流指定水位线
        SingleOutputStreamOperator<VisitorStats> unionWatermarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats visitorStats, long l) {
                        return visitorStats.getTs();
                    }
                }));

        // TODO 7.按照维度对流中的数据进行分组  版本、渠道、地区、新老访客
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorKeyedDS = unionWatermarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(
                        visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new()
                );
            }
        });

        // TODO 8. 对分组之后的数据进行开窗处理
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowsDS = visitorKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // TODO 9.对窗口内的数据进行聚合
        SingleOutputStreamOperator<VisitorStats> reduceVisitorDS = windowsDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats visitorStats, VisitorStats t1) throws Exception {
                        visitorStats.setSv_ct(visitorStats.getSv_ct() + t1.getSv_ct());
                        visitorStats.setDur_sum(visitorStats.getDur_sum() + t1.getDur_sum());
                        visitorStats.setPv_ct(visitorStats.getPv_ct() + t1.getPv_ct());
                        visitorStats.setUv_ct(visitorStats.getUv_ct() + t1.getUv_ct());
                        visitorStats.setUj_ct(visitorStats.getUj_ct() + t1.getUj_ct());
                        return visitorStats;
                    }
                },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> collector) throws Exception {
                        // 将时间字段补全
                        for (VisitorStats visitorStats : elements) {
                            visitorStats.setStt(DateTimeUtil.getTimeDateStr(new Date(context.window().getStart())));
                            visitorStats.setEdt(DateTimeUtil.getTimeDateStr(new Date(context.window().getEnd())));
                            visitorStats.setTs(System.currentTimeMillis());
                            collector.collect(visitorStats);
                        }
                    }
                }
        );

        reduceVisitorDS.print("****");

        // TODO 10.把聚合统计后的数据写回clickhouse
        // 由于这里流中的数据只写入一个table，因此可以使用flink提供的sink，
        // 而动态分流维度数据哪里由于一个流中的数据需要写到多个hbase表格，因此需要自己实现
        reduceVisitorDS.addSink(ClickHouseUtil.getSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }
}
