package com.xiang.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/13
 * Desc: 查询phoenix的表格数据，并封装成你指定的POJO类
 */
public class PhoenixUtil {
    public static void main(String[] args) throws InvocationTargetException, InstantiationException, IllegalAccessException {
        List<JSONObject> objectList = queryList("select * from  dim_base_trademark", JSONObject.class);
        System.out.println(objectList);
    }
    private static Connection conn;
    // 通过JDBC连接hbase，然后对于某个sql进行查询，对结果集进行封装成T对象，然后输出T对象的列表
    public static <T>List<T> queryList(String sql,Class<T> tClass) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<T> res = new ArrayList<>();
        if (conn == null){ // conn连接为空再去创建数据库连接
            init();
        }
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData(); // 获取数据库表格中的所有的列名
            while (resultSet.next()){
                T obj = tClass.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i); //按照列名取对应的数据
                    BeanUtils.setProperty(obj,columnName,resultSet.getObject(i)); //对obj的某个属性进行赋值
                }
                res.add(obj);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return res;
    }

    private static void init() {
        try {
            // TODO 1.注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            try {
                // TODO 2.获取数据库连接
                conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
                conn.setSchema(GmallConfig.HBASE_SCHEMA);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
