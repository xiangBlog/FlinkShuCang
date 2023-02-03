package com.xiang.gmall.realtime.app.dwd.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * User: 51728
 * Date: 2022/10/30
 * Desc: 自定义反序列化器
 */
public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema<String> {
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
