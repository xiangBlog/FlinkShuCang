package com.xiang.gmallpublisher.service;

import com.xiang.gmallpublisher.beans.VisitorStats;

import java.util.List;

public interface VisitorStatsService {
    List<VisitorStats> getVisitorStatsByNew(Integer date);

    List<VisitorStats> getVisitorTime(Integer date);
}
