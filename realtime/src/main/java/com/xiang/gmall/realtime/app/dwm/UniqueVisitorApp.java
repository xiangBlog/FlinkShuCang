package com.xiang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * User: 51728
 * Date: 2022/11/12
 * Desc:
 */
public class UniqueVisitorApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 检查点相关设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmallDB"));
        System.setProperty("HADOOP_USER_NAME","xiang");

        // 读取数据
        String topic = "dwd_page_log";
        String groupId = "dwm_unique_visitor_app";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtils.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONObject.parseObject(s);
            }
        });

        KeyedStream<JSONObject, String> keyByMidDS = jsonDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> filterDS = keyByMidDS.filter(
                new RichFilterFunction<JSONObject>() {
                    private ValueState<String> lastVisitorDateState;
                    private SimpleDateFormat sdf;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                        // 这里需要声明失效时间
                        ValueStateDescriptor<String> lastDateState = new ValueStateDescriptor<>("lastDateState", Types.STRING);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        lastDateState.enableTimeToLive(ttlConfig);
                        lastVisitorDateState = getRuntimeContext().getState(lastDateState);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageID = jsonObject.getJSONObject("page").getString("last_page_id");
                        if(lastPageID != null && lastPageID.length() > 0){
                            return false;
                        }
                        Long ts = jsonObject.getLong("ts");
                        String currDate = sdf.format(ts);
                        String lastDate = lastVisitorDateState.value();
                        // 如果日期不同，那么就更新状态变量，如果当前状态变量为空也要更新状态变量
                        // 如果状态变量不为空，且日期相等，证明该用户当天访问过
                        if(lastDate != null && lastDate.length() > 0 && lastDate.equals(currDate)){
                            // 满足该条件证明状态变量已经存在过数据，那么证明该用户访问过
                            return false;
                        }else{ // 状态变量不存在数据或者该用户当天没有访问过，就更新该数据
                            lastVisitorDateState.update(currDate);
                            return true;
                        }

                    }
                }
        );
        filterDS.print(">>>>");
        filterDS.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) throws Exception {
                return jsonObject.toJSONString();
            }
        }).addSink(
                MyKafkaUtils.getKafkaSink("dwm_unique_visitor")
        );
        env.execute();
    }
}
