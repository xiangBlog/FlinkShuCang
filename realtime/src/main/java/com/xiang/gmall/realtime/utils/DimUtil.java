package com.xiang.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/13
 * Desc: 这个工具类包装了如何从hbase的维度表中查询数据，用户指定一个key以及表格名称
 * 就可以调用方法去查询对应的数据，返回值是一个json对象，并且其中有的函数还做了缓存优化
 * 使用redis作为缓存的地方，把从hbase查询过的数据放到redis中去，下一次再从redis中读取的时候
 * 就不需要再从hbase中读取了，可以极大的增加查询的效率。
 * 这里为了保持redis和hbase中数据的一致性
 * 我们再写入hbase数据的时候检查是否是更新或者删除操作，如果是这些操作，我们需要把对应
 * 的redis中的缓存数据做删除操作，这样就可以保证数据之间的一致性
 */
public class DimUtil {
    public static void main(String[] args) throws InvocationTargetException, InstantiationException, IllegalAccessException {
        JSONObject jsonObject = queryDimHasCache("dim_base_trademark","13");
        System.out.println(jsonObject);
    }
    // 最原始的不适用cache的查询操作，不推荐使用
    public static JSONObject queryDimNoCache(String tableName, Tuple2<String,String> ... keyAndValue) throws InvocationTargetException, InstantiationException, IllegalAccessException {
        StringBuilder sql = new StringBuilder("select * from " + tableName + " where ");
        for (int i = 0; i < keyAndValue.length; i++) {
            Tuple2<String, String> oneKeyAndValue = keyAndValue[i];
            sql.append(oneKeyAndValue.f0).append("='").append(oneKeyAndValue.f1).append("'");
            if (i < keyAndValue.length -1) {
                sql.append(" and");
            }
        }
        System.out.println("SQL查询语句为："+sql);
        List<JSONObject> dimRes = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
        JSONObject jsonObject = null;
        if (dimRes.size() > 0){
            jsonObject = dimRes.get(0);
        }else {
            System.out.println("维度数据没有找到 " + sql);
        }
        return jsonObject;
    }
    // 进一层封装，维度表一般只有一个主键，简化用户使用函数的方便性，再一次封装
    public static JSONObject queryDimHasCache(String tableName,String value) throws InvocationTargetException, InstantiationException, IllegalAccessException {
        return queryDimHasCache(tableName,Tuple2.of("id",value));
    }
    // 封装了带有cache的查询过程
    public static JSONObject queryDimHasCache(String tableName, Tuple2<String,String> ... keyAndValue) throws InvocationTargetException, InstantiationException, IllegalAccessException {
        StringBuilder sql = new StringBuilder("select * from " + tableName + " where ");
        StringBuilder redisKey = new StringBuilder("dim:"+tableName.toLowerCase()+":");

        for (int i = 0; i < keyAndValue.length; i++) {
            Tuple2<String, String> oneKeyAndValue = keyAndValue[i];
            redisKey.append(oneKeyAndValue.f1);
            sql.append(oneKeyAndValue.f0).append("='").append(oneKeyAndValue.f1).append("'");
            if (i < keyAndValue.length -1) {
                redisKey.append("_");
                sql.append(" and");
            }
        }
        Jedis jedis = null;
        String redisDimData = null;
        JSONObject jsonObject = null;
        try {
            // 获取jedis
            System.out.println("----获取redis连接-----");
            jedis = RedisUtil.getJedis();
            // 获取redis中的缓存数据
            redisDimData = jedis.get(redisKey.toString());
        }catch (Exception e){
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
        if (redisDimData !=null && redisDimData.length()>0){
            // 如果redis中有缓存数据
            jsonObject = JSONObject.parseObject(redisDimData);
        }else {
            // 如果redis中没有缓存数据，需要取hbase中查询，并写入redis
            System.out.println("SQL查询语句为："+sql);
            List<JSONObject> dimRes = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
            if (dimRes.size() > 0){
                jsonObject = dimRes.get(0);
                if (jedis != null) {
                    jedis.setex(redisKey.toString(),3600*24,jsonObject.toJSONString());
                }
            }else {
                System.out.println("维度数据没有找到 " + sql);
            }
        }
        if(jedis != null){
            jedis.close();
            System.out.println("----关闭redis连接----");
        }
        return jsonObject;
    }
    // 删除缓存中的数据
    public static void deleteRedisCache(String tableName, String value) {
        String redisKey = new String("dim:"+tableName.toLowerCase()+":"+value);

        try{
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
            System.out.println("Redis缓存中的key被删除  "+ redisKey);
            jedis.close();
        }catch (Exception e){
            System.out.println("清除缓存发生了错误");
        }

    }
}
