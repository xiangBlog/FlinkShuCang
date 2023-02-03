package com.xiang.gmallpublisher.service.impl;

import com.xiang.gmallpublisher.beans.ProductStats;
import com.xiang.gmallpublisher.mapper.ProductStatsMapper;
import com.xiang.gmallpublisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/17
 * Desc:
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService{
    @Autowired
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        BigDecimal res = productStatsMapper.selectGMV(date);
        return res;
    }

    @Override
    public List<ProductStats> getProductStatsByTm(Integer date, Integer limit) {
        List<ProductStats> productStatsList = productStatsMapper.selectProductStatsByTm(date, limit);
        return productStatsList;
    }

    @Override
    public List<ProductStats> getProductStatsByCate(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByCate(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsBySPU(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsBySpu(date,limit);
    }
}
