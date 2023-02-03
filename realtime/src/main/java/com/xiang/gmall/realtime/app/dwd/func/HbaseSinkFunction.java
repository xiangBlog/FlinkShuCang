package com.xiang.gmall.realtime.app.dwd.func;

import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.common.GmallConfig;
import com.xiang.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.mortbay.util.StringUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * User: 51728
 * Date: 2022/10/31
 * Desc:
 */
public class HbaseSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String sink_table = value.getString("sink_table");
        JSONObject dataJsonObj = value.getJSONObject("data");
        String upsertSql = genUpsertSql(sink_table, dataJsonObj);
        PreparedStatement preparedStatement = conn.prepareStatement(upsertSql);
        try {
            System.out.println("upsert语句为  "+ upsertSql);
            preparedStatement.executeUpdate();
            conn.commit();
        } catch (Exception e){
            throw new Exception("插入数据失败");
        } finally {
            preparedStatement.close();
        }
        // 如果维度数据进行更新或删除
        if(value.getString("type").equals("update") ||
                value.getString("type").equals("delete")){
            // 删除缓存中的数据
            DimUtil.deleteRedisCache(sink_table,dataJsonObj.getString("id"));
        }

    }

    private String genUpsertSql(String sink_table, JSONObject dataJsonObj) {
        StringBuilder upsertSql = new StringBuilder();
        upsertSql.append("upsert into ").append(GmallConfig.HBASE_SCHEMA).append(".").append(sink_table);
        upsertSql.append(" (");

        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();
        String keysWith = StringUtils.join(keys, ",");
        String valuesWith = StringUtils.join(values, "','");

        upsertSql.append(keysWith).append(") ").append("values('").append(valuesWith).append("')");

        return upsertSql.toString();
    }
}
