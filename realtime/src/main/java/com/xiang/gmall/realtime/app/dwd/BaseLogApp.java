package com.xiang.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * User: 51728
 * Date: 2022/10/28
 * Desc: 对日志数据进行分流操作
 * 启动日志、曝光日志、页面日志
 * 启动日志放到启动测输出流
 * 曝光日志放到曝光测输出流
 * 页面日志放到主流
 * 把不同的流协会kafka dwd主题中
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception{
        //TODO 1.基本环境
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.2 设置并行度
        //TODO 2.检查点相关操作
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //2.3 设置重启的策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.4 设置job取消之后，检查点是否还保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmall"));
        //2.6 指定操作HDFS的用户
        System.setProperty("HADOOP_USER_NAME","xiang");

        //TODO 3.从Kafka中读取数据
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getKafkaSource(topic, groupId));
        //TODO 4.对读取的数据进行结构的转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        return JSON.parseObject(s);
                    }
                }
        );
        //TODO 5.新老访客状态进行修复
        KeyedStream<JSONObject, String> keyDS = jsonObjectDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> jsonWithIsNew = keyDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastVisitDateState = getRuntimeContext().getState(
                        new ValueStateDescriptor<String>("lastVisitState", Types.STRING)
                );
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    String curVisitDate = sdf.format(jsonObject.getLong("ts"));
                    String lastVisitDate = lastVisitDateState.value();
                    if (lastVisitDate != null && lastVisitDate.length() > 0) {
                        if (!curVisitDate.equals(lastVisitDate)) {
                            jsonObject.getJSONObject("common").put("is_new", "0");
                        }
                    } else {
                        lastVisitDateState.update(curVisitDate);
                    }
                }
                return jsonObject;
            }
        });
        //TODO 6.按照日志类型对日志进行分流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> pageDS = jsonWithIsNew.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        JSONObject start = jsonObject.getJSONObject("start");
                        String ts = jsonObject.getString("ts");
                        String jsonString = jsonObject.toJSONString();
                        if (start != null && start.size() > 0) {
                            context.output(startTag, jsonString);
                        } else {
                            collector.collect(jsonString);
                            String pageId = jsonObject.getJSONObject("page").getString("page_id");
                            JSONArray display = jsonObject.getJSONArray("displays");
                            if (display != null && display.size() > 0) {
                                for (int i = 0; i < display.size(); i++) {
                                    JSONObject displayJSONObject = display.getJSONObject(i);
                                    displayJSONObject.put("ts", ts);
                                    displayJSONObject.put("page_id", pageId);
                                    String displayJSONStr = displayJSONObject.toJSONString();
                                    context.output(displayTag, displayJSONStr);
                                }
                            }
                        }
                    }
                }
        );
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //TODO 7.把不同的流写到Kafka的dwd的不同主题中去

        pageDS.addSink(MyKafkaUtils.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtils.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtils.getKafkaSink("dwd_display_log"));

        env.execute();
    }

}
