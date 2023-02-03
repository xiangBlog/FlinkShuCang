package com.xiang.gmall.realtime.app.dws;

import com.xiang.gmall.realtime.app.dws.func.KeyWordUDTF;
import com.xiang.gmall.realtime.beans.KeyWordStats;
import com.xiang.gmall.realtime.common.GmallConstant;
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
public class KeyWordStatsApp {
    public static void main(String[] args) throws Exception{
        // TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        env.setParallelism(4);

        // TODO 2.检查点并且注册UDTF函数
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/checkpoint/gmall"));
        System.setProperty("HADOOP_USER_NAME","xiang");

        tableEnvironment.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);

        // TODO 3.转换动态表
        String topic = "dwd_page_log";
        String groupId = "key_word_app";
        tableEnvironment.executeSql("create table page_view( " +
                "common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>, " +
                "ts BIGINT, " +
                "order_time AS to_timestamp(FROM_UNIXTIME(ts/1000))," +
                "WATERMARK FOR order_time AS order_time - INTERVAL '3' SECOND" +
                ") WITH (" + MyKafkaUtils.getKafkaDDL(topic,groupId) +")"
        );

        // TODO 4.过滤出来搜索行为记录

        Table fullWordTable = tableEnvironment.sqlQuery("select" +
                "   page['item'] fullword," +
                "   order_time " +
                "from" +
                "   page_view " +
                "where" +
                "   page['page_id']='good_list' and page['item'] is not null");

        // TODO 5.使用UDTF拆分搜索的词

        Table keyWordTable = tableEnvironment.sqlQuery("SELECT  keyword,order_time " +
                "FROM " + fullWordTable + ", LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)");

        // TODO 6.分组、开窗、聚合
        Table resTable = tableEnvironment.sqlQuery(
                "select " +
                        "  DATE_FORMAT(TUMBLE_START(order_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt, " +
                        "  DATE_FORMAT(TUMBLE_END(order_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt, " +
                        "  keyword, " +
                        "  COUNT(*) ct, " +
                        "  '" + GmallConstant.KEYWORD_SEARCH + "' source, " +
                        "  UNIX_TIMESTAMP()*1000 ts  " +
                        "from " +
                        "  " + keyWordTable + " " +
                        "group by " +
                        "  TUMBLE(order_time, INTERVAL '10' SECOND), " +
                        "  keyword");

        //TODO 7.转换为动态表
        DataStream<KeyWordStats> resDS = tableEnvironment.toAppendStream(resTable, KeyWordStats.class);

        //TODO 8.写入ClickHouse
        resDS.addSink(ClickHouseUtil.getSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        env.execute();
    }
}
