package com.xiang.gmall.realtime.utils;

import com.xiang.gmall.realtime.beans.TransientSink;
import com.xiang.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * User: 51728
 * Date: 2022/11/15
 * Desc: Clickhouse工具类
 *
 *
 */
public class ClickHouseUtil {

    // 该方法提供clickhouse的sink函数
    // 我看phoenixUtil中函数还引入了T的class参数，这里面没给的原因是后面传入了T的对象可以获得
    public static <T> SinkFunction <T> getSink(String sql){
        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // 该函数需要实现的功能：把t对象赋值到sql的占位符中去
                        // 获取所有属性名称
                        Field[] fields = t.getClass().getDeclaredFields();
                        int skipNum = 0;
                        for (int i = 0; i < fields.length; i++) {
                            // 遍历获取一个属性名称
                            Field field = fields[i];
                            // 该属性设置可修改
                            field.setAccessible(true);
                            // 获取注解名称
                            TransientSink fieldAnnotation = field.getAnnotation(TransientSink.class);
                            if (fieldAnnotation != null) {
                                skipNum++;
                                continue;
                            }
                            // 获取该属性对应的值
                            Object value = null;
                            try {
                                value = field.get(t);
                                // 把该属性值写入占位符
                                preparedStatement.setObject(i+1-skipNum,value);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(10000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }

}
