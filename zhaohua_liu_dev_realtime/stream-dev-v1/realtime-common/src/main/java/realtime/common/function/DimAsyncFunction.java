package realtime.common.function;

import com.alibaba.fastjson.JSONObject;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import realtime.common.bean.DimJoinFunction;
import realtime.common.constant.Constant;
import realtime.common.util.HbaseUtil;
import realtime.common.util.RedisUtil;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 发送异步请求进行维度关联的模板类
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
    //成员变量:HBase异步连接
    private AsyncConnection hbaseAsyncConn;
    //成员变量:redis异步连接
    private StatefulRedisConnection<String,String> redisAsyncConn;

    //在 Flink 任务启动时初始化 HBase 和 Redis 的异步连接
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseAsyncConn = HbaseUtil.getHbaseAsyncConnection();
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
    }

    //在任务结束时关闭连接，释放资源，防止内存泄漏。
    @Override
    public void close() throws Exception {
        HbaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
    }

    //核心方法,obj是上游传来的数据,ResultFuture<T> resultFuture 参数是由 Flink 框架自动创建并传入的，用于处理异步操作的结果
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //创建异步编排对象  执行线程任务，有返回值
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    //get() 方法是 Supplier<JSONObject> 函数式接口的实现方法
                    @Override
                    public JSONObject get() {
                        //根据当前流中对象获取要关联的维度的主键
                        String key = getRowKey(obj);
                        //根据维度的主键到Redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, getTableName(), key);
                        return dimJsonObj;
                    }
                }
        //有入参、有返回值   ，上一个线程任务的返回值将作为当前线程任务的入参
        ).thenApplyAsync(
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        if(dimJsonObj != null){
                            //如果在Redis中找到了要关联的维度(缓存命中)。 直接将命中的维度作为结果返回
                            System.out.println("~~~从Redis中找到了"+getTableName()+"表的"+getRowKey(obj)+"数据~~~");
                        }else{
                            //如果在Redis中没有找到要关联的维度，发送请求到HBase中查找
                            dimJsonObj = HbaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE,getTableName(),getRowKey(obj));
                            //将查找到的维度数据放到Redis中缓存起来，方便下次查询使用
                            if(dimJsonObj != null){
                                System.out.println("~~~从HBase中找到了"+getTableName()+"表的"+getRowKey(obj)+"数据~~~");
                                RedisUtil.writeDimAsync(redisAsyncConn,getTableName(),getRowKey(obj),dimJsonObj);
                            }else{
                                System.out.println("~~~没有找到"+getTableName()+"表的"+getRowKey(obj)+"数据~~~");
                            }

                        }
                        return dimJsonObj;
                    }
                }
        //有入参，无返回值
        ).thenAcceptAsync(
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        if(dimJsonObj != null){
                            //将维度对象相关的维度属性补充到流中对象上
                            addDims(obj,dimJsonObj);
                        }
                        //获取数据库交互的结果并发送给ResultFuture的回调函数，将关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );
    }
}
