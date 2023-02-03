package com.xiang.gmallpublisher.service;

import com.xiang.gmallpublisher.beans.ProductStats;
import com.xiang.gmallpublisher.beans.ProvinceStats;

import java.util.List;

public interface ProvinceStatsService {

    List<ProvinceStats> getProvinceStats(Integer date);
}
