package com.xiang.gmall.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * User: 51728
 * Date: 2022/11/14
 * Desc: 从时间转换为时间戳
 * 由于simpleDateFormat 封装工具类的时候 会产生线程不安全的问题
 *
 */
public class DateTimeUtil {
    // private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String getTimeDateStr(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }
    public static Long getTimeTs(String dateStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr,dtf);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

}
