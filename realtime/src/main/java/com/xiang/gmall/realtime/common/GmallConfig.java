package com.xiang.gmall.realtime.common;

import com.xiang.gmall.realtime.beans.ProductStats;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * User: 51728
 * Date: 2022/10/31
 * Desc:
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";

    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop101:8123/default";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

    }
}
