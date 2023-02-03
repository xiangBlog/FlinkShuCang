package com.xiang.gmall.realtime.utils;

import com.mysql.jdbc.TimeUtil;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: 51728
 * Date: 2022/11/13
 * Desc: 定义了线程池的概念，为了增加flink使用异步io来提升效率，在此处需要定义一个获取线程池的方法类
 */
public class ThreadUtil {
    public static volatile ThreadPoolExecutor poolExecutor;

    // 使用懒汉式的单例模式，为了解决线程安全问题，使用双重锁机制
    public static ThreadPoolExecutor getInstance(){
        if (poolExecutor == null){
            synchronized (ThreadPoolExecutor.class){
                if(poolExecutor == null){
                    System.out.println("----开辟线程池----");
                    poolExecutor = new ThreadPoolExecutor(
                            4,10,300, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return poolExecutor;
    }
}
