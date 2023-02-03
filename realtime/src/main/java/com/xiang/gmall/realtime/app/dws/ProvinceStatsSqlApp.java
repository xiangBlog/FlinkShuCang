package com.xiang.gmall.realtime.app.dws;

import com.xiang.gmall.realtime.beans.ProvinceStats;
import com.xiang.gmall.realtime.utils.ClickHouseUtil;
import com.xiang.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * User: 51728
 * Date: 2022/11/16
 * Desc:
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception{
        // TODO 1.环境准备
        // 流处理环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // 设置并行度
        streamEnv.setParallelism(4);

        // TODO 2.检查点设置
        streamEnv.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        streamEnv.getCheckpointConfig().setCheckpointTimeout(60000L);
        streamEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        streamEnv.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmall"));
        System.setProperty("HADOOP_USER_NAME","xiang");

        // TODO 3.从指定数据源读取数据 转换为动态表、
        // 创建动态表
        String topic = "dwm_order_wide";
        String groupId = "province_stats_sql_app";
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE ( " +
                "province_id BIGINT, " +
                "province_name STRING," +
                "province_area_code STRING," +
                "province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id STRING," +
                "split_total_amount DOUBLE," +
                "create_time STRING," +
                "rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR rowtime AS rowtime" +
                ") WITH ( "+ MyKafkaUtils.getKafkaDDL(topic,groupId) +"" +
                ")");
        // TODO 4.开窗、聚合、分组

        Table table = tableEnv.sqlQuery("select" +
                "   DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt," +
                "   DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt," +
                "   province_id," +
                "   province_name," +
                "   province_area_code area_code," +
                "   province_iso_code iso_code ," +
                "   province_3166_2_code iso_3166_2 ," +
                "   COUNT( DISTINCT  order_id) order_count, " +
                "   sum(split_total_amount) order_amount," +
                "   UNIX_TIMESTAMP()*1000 ts " +
                "from" +
                "   ORDER_WIDE " +
                "group by" +
                "   TUMBLE(rowtime, INTERVAL '10' SECOND)," +
                "   province_id,province_name,province_area_code,province_iso_code,province_3166_2_code");


        // TODO 5.转换成流
        DataStream<ProvinceStats> resDS = tableEnv.toAppendStream(table, ProvinceStats.class);

        resDS.print("data%%%");
        // TODO 6.写入clickhouse
        resDS.addSink(
                ClickHouseUtil.getSink("insert into  province_stats values(?,?,?,?,?,?,?,?,?,?)")
        );

        streamEnv.execute();
    }
}
