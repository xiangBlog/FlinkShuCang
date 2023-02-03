package com.xiang.gmallpublisher.service.impl;

import com.xiang.gmallpublisher.beans.ProductStats;
import com.xiang.gmallpublisher.beans.ProvinceStats;
import com.xiang.gmallpublisher.mapper.ProvinceStatsMapper;
import com.xiang.gmallpublisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/17
 * Desc:
 */
@Service
public class ProvinceStatsServerImpl implements ProvinceStatsService {
    @Autowired
    private ProvinceStatsMapper provinceStatsMapper;
    @Override
    public List<ProvinceStats> getProvinceStats(Integer date) {
        return provinceStatsMapper.selectProvinceOrderAmount(date);
    }
}
