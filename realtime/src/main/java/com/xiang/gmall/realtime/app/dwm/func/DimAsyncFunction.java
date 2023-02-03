package com.xiang.gmall.realtime.app.dwm.func;

import com.alibaba.fastjson.JSONObject;
import com.xiang.gmall.realtime.utils.DimUtil;
import com.xiang.gmall.realtime.utils.ThreadUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * User: 51728
 * Date: 2022/11/13
 * Desc: 该方法定义了如何使用异步IO的方式对维度表进行join操作
 * 模板方法设计模式，再父类中定义实现某一个功能的核心算法骨架，具体实现在子类中实现
 * 子类在不改变代码框架的前提下，可以实现特定的功能
 */
// 泛型的定义，如果是定义在方法中，那么需要在方法名的前面加上<T>，如果是类名处，那么在类名后面加上<T>
// 模板方法设计模式，没有固定参数，需要用户去自己指定
public abstract class  DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoin<T>{
    private ExecutorService executorService;
    private String tableName;
    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadUtil.getInstance();
    }

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    // 发布异步请求，完成维度关联
    // 通过创建多线程的方式  发送异步请求
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        // submit操作就是相当于从线程池拿出一个线程去执行维度表的关联操作
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 代码我知道要做什么，但是不知道要怎么做，需要使用模板方法设计模式
                            // 在run中的代码就是异步维度关联的最核心代码
                            // 需要从对象中获取维度关联的key
                            Long start = System.currentTimeMillis();
                            String key = getKey(obj); // 抽象方法
                            // 利用key和关联的维度表表明去Hbase中查询维度表对应的信息
                            JSONObject jsonObject = DimUtil.queryDimHasCache(tableName, key);
                            System.out.println("从维度表中查询出来的json对象为 " + jsonObject);
                            // 最后把查询出来的信息插入到对象中
                            if (jsonObject != null){
                                join(obj, jsonObject);
                            }
                            Long end = System.currentTimeMillis();
                            System.out.println("异步查询耗时 " + (end - start) + " 毫秒");
                            resultFuture.complete(Arrays.asList(obj));
                        }catch (Exception e){
                            System.out.println(String.format(tableName+"异步查询异常. %s", e));
                            e.printStackTrace();
                        }
                    }
                }
        );
    }

}
