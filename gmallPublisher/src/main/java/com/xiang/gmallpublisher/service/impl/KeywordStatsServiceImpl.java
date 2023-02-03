package com.xiang.gmallpublisher.service.impl;

import com.xiang.gmallpublisher.beans.KeywordStats;
import com.xiang.gmallpublisher.mapper.KeyWordStatsMapper;
import com.xiang.gmallpublisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/17
 * Desc:
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
    @Autowired
    private KeyWordStatsMapper keyWordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(Integer date, Integer limit) {
        return keyWordStatsMapper.selectKeywordStats(date,limit);
    }
}
