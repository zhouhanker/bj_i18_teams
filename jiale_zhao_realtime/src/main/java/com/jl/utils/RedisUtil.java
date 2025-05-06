package com.jl.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class RedisUtil {
    private static JedisPool jedisPool;
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(200);
        jedisPoolConfig.setMaxIdle(50);
        jedisPoolConfig.setMinIdle(10);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        String password = "123456";

        jedisPool = new JedisPool(jedisPoolConfig, "cdh03", 6379 , 10000, password);
    }

    // 获取Jedis
    public static Jedis getJedis() {
        System.out.println("~~~获取Jedis客户端~~~");
        try {
            return jedisPool.getResource();
        } catch (Exception e) {
            System.err.println("获取 Jedis 连接失败: " + e.getMessage());
            return null;
        }
    }

    // 关闭Jedis
    public static void closeJedis(Jedis jedis) {
        System.out.println("~~~关闭Jedis客户端~~~");
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                System.err.println("关闭 Jedis 连接失败: " + e.getMessage());
            }
        }
    }

    // 关闭连接池
    public static void closePool() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }

    //获取异步操作Redis的连接对象
    public static StatefulRedisConnection<String,String> getRedisAsyncConnection(){
        System.out.println("~~~获取异步操作Redis的客户端~~~");
        RedisClient redisClient = RedisClient.create("redis://123456@cdh03:6379/");
        return redisClient.connect();
    }
    //关闭异步操作Redis的连接对象
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String,String> asyncRedisConn){
        System.out.println("~~~关闭异步操作Redis的客户端~~~");
        if(asyncRedisConn != null && asyncRedisConn.isOpen()){
            asyncRedisConn.close();
        }
    }

    //以异步的方式从Redis中取数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String,String> asyncRedisConn,String tableName,String id){
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        String key = getKey(tableName, id);
        try {
            String dimJsonStr = asyncCommands.get(key).get();
            if(StringUtils.isNotEmpty(dimJsonStr)){
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    //以异步的方式向Redis中放数据
    public static void writeDimAsync(StatefulRedisConnection<String,String> asyncRedisConn,String tableName,String id,JSONObject dimJsonObj){
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        String key = getKey(tableName, id);
        asyncCommands.setex(key,24*60*60,dimJsonObj.toJSONString());

    }

    //从Redis中取数据
    public static JSONObject readDim(Jedis jedis,String tableName,String id){
        //拼接key
        String  key = getKey(tableName, id);
        //根据key到Redis中获取维度数据
        String dimJsonStr = jedis.get(key);
        if(StringUtils.isNotEmpty(dimJsonStr)){
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }

    public static String getKey(String tableName, String id) {
        String key = tableName + ":" + id;
        return key;
    }
    //向Redis中放数据
    public static void writeDim(Jedis jedis,String tableName,String id,JSONObject dimJsonObj){
        String key = getKey(tableName, id);
        jedis.setex(key,24*60*60,dimJsonObj.toJSONString());
    }



    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        closeJedis(jedis);
    }
}
