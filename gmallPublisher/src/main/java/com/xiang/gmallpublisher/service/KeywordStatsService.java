package com.xiang.gmallpublisher.service;

import com.xiang.gmallpublisher.beans.KeywordStats;

import java.util.List;

public interface KeywordStatsService {

    List<KeywordStats> getKeywordStats(Integer date,Integer limit);
}
