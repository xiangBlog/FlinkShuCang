package com.xiang.gmallpublisher.service.impl;

import com.xiang.gmallpublisher.beans.VisitorStats;
import com.xiang.gmallpublisher.mapper.VisitorStatsMapper;
import com.xiang.gmallpublisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/17
 * Desc:
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {
    @Autowired
    private VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorTime(Integer date) {
        return visitorStatsMapper.selectVisitorTime(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByNew(Integer date) {
        return visitorStatsMapper.selectVisitorStats(date);
    }
}
