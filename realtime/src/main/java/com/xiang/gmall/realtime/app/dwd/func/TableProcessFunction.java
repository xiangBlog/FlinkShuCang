package com.xiang.gmall.realtime.app.dwd.func;

import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.beans.TableProcess;
import com.xiang.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * User: 51728
 * Date: 2022/10/30
 * Desc:
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> dimTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //注册驱动
        Class. forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //创建数据库操作对象
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    public TableProcessFunction(OutputTag<JSONObject> dimTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimTag = dimTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");

        // 使用Maxwell处理历史数据的时候，类型是bootstrap-insert  这里需要把该类型修复为insert
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObject.put("type", type);
        }

        String key = table + ":" + type;
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            String sinkTable = tableProcess.getSinkTable();
            jsonObject.put("sink_table",sinkTable);
            // 在向下游传递数据之前，把不需要的字段过滤掉 从配置表格中读取需要保留的字段，然后根据保留的字段，对于data中的属性进行保留
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            filterColumns(dataJsonObj, tableProcess.getSinkColumns());
            String sinkType = tableProcess.getSinkType();
            if(sinkType.equals(TableProcess.SINK_TYPE_HBASE)){
                //维度数据
                readOnlyContext.output(dimTag,jsonObject);
            } else if (sinkType.equals(TableProcess.SINK_TYPE_KAFKA)) {
                //事实数据
                collector.collect(jsonObject);
            } else {
                System.out.println("The program has something wrong with coding error");
            }
        }else {
            System.out.println("No this key in this table_process  " + key);
        }
        
    }

    private void filterColumns(JSONObject dataJsonObj, String sinkColumns) {
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(new Predicate<Map.Entry<String, Object>>() {
            @Override
            public boolean test(Map.Entry<String, Object> stringObjectEntry) {
                return !sinkColumns.contains(stringObjectEntry.getKey());
            }
        });

    }

    // 处理配置表格数据
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        JSONObject jsonObject = JSONObject.parseObject(s);
        // 获取data对象，注意不是json对象
        TableProcess dataObj = JSONObject.parseObject(jsonObject.getString("data"), TableProcess.class);
        // 操作类型
        String operateType = dataObj.getOperateType();
        // 指定数据输出目的地
        String sourceTable = dataObj.getSourceTable();

        String sinkTable = dataObj.getSinkTable();
        String sinkType = dataObj.getSinkType();
        String sinkColumns = dataObj.getSinkColumns();
        String sinkExtend = dataObj.getSinkExtend();
        String sinkPk = dataObj.getSinkPk();

        if(TableProcess.SINK_TYPE_HBASE.equals(sinkType) && operateType.equals("insert")){
            checkTable(sinkTable, sinkColumns, sinkExtend,sinkPk);
        }

        String key = sourceTable + ":" + operateType;

        broadcastState.put(key,dataObj);

    }

    private void checkTable(String tableName, String fields, String ext, String primaryKey) {
        if (primaryKey == null) {
            primaryKey = "id";
        }
        if(ext == null){
            ext = "";
        }
        StringBuilder createSql = new StringBuilder("create table if not exists " +
                GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] allFields = fields.split(",");
        for (int i = 0; i < allFields.length; i++) {
            if(allFields[i].equals(primaryKey)){
                createSql.append(allFields[i]).append(" varchar primary key ");
            }else {
                createSql.append(allFields[i]).append(" varchar ");
            }
            if( i < allFields.length -1){
                createSql.append(",");
            }
        }
        createSql.append(")").append(ext);
        System.out.println("利用phoenix创建hbase表格:"+createSql);

        //执行SQL语句
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        //处理结果集合
        try {
            boolean b = preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("在phoenix创建表格失败");
        } finally {
          if(preparedStatement != null){
              try {
                  preparedStatement.close();
              } catch (SQLException e) {
                  throw new RuntimeException(e);
              }
          }
        }
    }

}
