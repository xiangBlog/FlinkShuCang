package com.xiang.gmall;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


/**
 * User: 51728
 * Date: 2022/10/30
 * Desc:
 * 返回值通过反序列化器，得到的结果不是我们想要的结果，因此我们需要自定义一个反序列化器
 */
public class FlinkCDC_DS {
    public static void main(String[] args) throws Exception{
        // 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //检查点设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/flinkCDC"));
        System.setProperty("HADOOP_USER_NAME","xiang");

        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .databaseList("gmall2022_realtime") // set captured database
                .tableList("gmall2022_realtime.table_process") // set captured table
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MySchema()) // converts SourceRecord to JSON String
                .build();

        env.addSource(sourceFunction).print().setParallelism(1);
        env.execute();

    }
}

class MySchema implements DebeziumDeserializationSchema<String>{
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct values = (Struct) sourceRecord.value();
        Struct sourceStruct = values.getStruct("source");
        String db = sourceStruct.getString("db");
        String table = sourceStruct.getString("table");

        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if(type.equals("create")){
            type = "insert";
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("database",db);
        jsonObject.put("table",table);
        jsonObject.put("type",type);

        Struct afterStruct = values.getStruct("after");
        JSONObject dataObject = new JSONObject();
        if (afterStruct != null) {
            List<Field> fields = afterStruct.schema().fields();

            for(Field field: fields){
                dataObject.put(field.name(),afterStruct.get(field));
            }
        }
        jsonObject.put("data",dataObject);
        collector.collect(jsonObject.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
