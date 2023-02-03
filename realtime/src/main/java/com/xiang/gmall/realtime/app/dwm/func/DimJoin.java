package com.xiang.gmall.realtime.app.dwm.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoin<T> {
    public abstract void join(T obj, JSONObject jsonObject) throws Exception;

    public abstract String getKey(T obj);
}
