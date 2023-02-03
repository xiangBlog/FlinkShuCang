package com.xiang.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * User: 51728
 * Date: 2022/11/13
 * Desc: 获取操作Redis的Java客户端 Jedis
 */
public class RedisUtil {
    private static JedisPool jedisPool = null;
    public static Jedis getJedis(){
        if (jedisPool == null){
            initJedisPool();
        }
        return jedisPool.getResource();
    }

    private static void initJedisPool() {
        // 连接池配置对象
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 最大连接数
        jedisPoolConfig.setMaxTotal(100);
        // 每次在连接的时候是否进行ping pong测试
        jedisPoolConfig.setTestOnBorrow(true);
        // 连接耗尽是否等待,并设置等待时间
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        // 最小空闲连接数
        jedisPoolConfig.setMinIdle(5);
        // 最大空闲连接数
        jedisPoolConfig.setMaxIdle(10);
        jedisPool = new JedisPool(jedisPoolConfig,"hadoop101",6379,10000);
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
    }
}
