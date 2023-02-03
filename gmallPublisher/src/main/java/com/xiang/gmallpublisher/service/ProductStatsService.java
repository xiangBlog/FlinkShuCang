package com.xiang.gmallpublisher.service;

import com.xiang.gmallpublisher.beans.ProductStats;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsService {
    // 获取某天的总交易额
    public BigDecimal getGMV(Integer date);

    // 获取品牌对应的成交额
    List<ProductStats> getProductStatsByTm(Integer date, Integer limit);

    List<ProductStats> getProductStatsByCate(Integer date,Integer limit);

    List<ProductStats> getProductStatsBySPU(Integer date,Integer limit);
}
