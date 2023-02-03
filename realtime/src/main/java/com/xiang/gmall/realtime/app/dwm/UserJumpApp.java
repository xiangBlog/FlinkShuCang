package com.xiang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * User: 51728
 * Date: 2022/11/12
 * Desc:
 */
public class UserJumpApp {
    public static void main(String[] args) throws Exception{
        // TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2.检查点设置
        // 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置超时策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        // 设置任务结束后是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置保存位置
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmall"));
        // 设置操作用户
        System.setProperty("HADOOP_USER_NAME","xiang");

        // TODO 3.从Kafka中读取数据
        String topic = "dwd_page_log";
        String groupId = "dwm_user_jump_app";
        DataStreamSource<String> kafkaDS = env.addSource(
                MyKafkaUtils.getKafkaSource(topic, groupId)
        );

        // TODO 4.转换数据格式
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        return JSONObject.parseObject(s);
                    }
                }
        );

        // TODO 5.定义水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkDS = jsonDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })
        );

        // TODO 6.分组
        KeyedStream<JSONObject, String> keyedDS = withWaterMarkDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        // TODO 7.定义一个Pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) {
                        String last_page_id = jsonObject.getJSONObject("page").getString("last_page_id");
                        return last_page_id == null || last_page_id.length() == 0;
                    }
                }
        ).next("second").subtype(JSONObject.class).where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        return pageId != null && pageId.length() > 0;
                    }
                }
        ).within(Time.seconds(1));

        // TODO 8.把模式应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);

        // TODO 9.从流中取出数据
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
        SingleOutputStreamOperator<String> resDS = patternDS.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        List<JSONObject> firsts = map.get("first");
                        for (JSONObject jsonObj : firsts) {
                            collector.collect(jsonObj.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {

                    }
                }
        );

        // TODO 10.把测输出流数据写入Kafka
        DataStream<String> timeoutDS = resDS.getSideOutput(timeoutTag);
        timeoutDS.print("jump:");
        timeoutDS.addSink(MyKafkaUtils.getKafkaSink("dwm_user_jump_detail"));
        env.execute();

    }
}
