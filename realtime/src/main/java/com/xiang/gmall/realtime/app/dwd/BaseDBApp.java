package com.xiang.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.xiang.gmall.realtime.app.dwd.func.HbaseSinkFunction;
import com.xiang.gmall.realtime.app.dwd.func.TableProcessFunction;
import com.xiang.gmall.realtime.beans.TableProcess;
import com.xiang.gmall.realtime.app.dwd.func.MyDeserializationSchemaFunction;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * User: 51728
 * Date: 2022/10/30
 * Desc:
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception{
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmallDB"));
        System.setProperty("HADOOP_USER_NAME","xiang");
        //TODO 3.从Kafka读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        DataStreamSource<String> KafkaDS = env.addSource(MyKafkaUtils.getKafkaSource(topic, groupId));

        //TODO 4.对数据类型进行转换  String -> JSONObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = KafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        });
        //TODO 5.简单的数据清洗
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("table") != null
                                && jsonObject.getString("table").length() > 0
                                && jsonObject.getJSONObject("data") != null
                                && jsonObject.getString("data").length() > 3;

                    }
                }
        );

        //TODO 6.使用FlinkCDC来读取数据
        DebeziumSourceFunction<String> mySQLSourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .databaseList("gmall2022_realtime") // set captured database
                .tableList("gmall2022_realtime.table_process") // set captured table
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchemaFunction()) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> mySqlDS = env.addSource(mySQLSourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
                "table_process",
                String.class,
                TableProcess.class
        );

        // 这一步不是很理解，感觉定义的状态描述器执行逻辑完全没有，应该是交给了下游去执行
        // 这一步是描述使用什么样子的结构去保存这个广播数据，保存逻辑等需要后续自己去定义
        BroadcastStream<String> broadcastStream = mySqlDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        //TODO 7.动态分流

        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {};

        SingleOutputStreamOperator<JSONObject> realDS = connectedStream.process(new TableProcessFunction(dimTag,mapStateDescriptor));
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        realDS.print(">>>");
        dimDS.print("$$$");

        //TODO 8.把维度测输出流写入HBase
        dimDS.addSink(new HbaseSinkFunction());
        //TODO 9.把主流数据写回Kafka
        realDS.addSink(MyKafkaUtils.getKafkaBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                        return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sink_table"),
                                jsonObject.getJSONObject("data").toJSONString().getBytes(StandardCharsets.UTF_8));
                    }
                }
        ));

        env.execute();
    }
}
