package com.xiang.gmallpublisher.mapper;

import com.xiang.gmallpublisher.beans.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface KeyWordStatsMapper {

    @Select("select keyword,　sum(keyword_stats.ct *　multiIf(　source='SEARCH',10,　source='ORDER',5,　source='CART',2,　source='CLICK',1,0　)) ct　from　keyword_stats　where　toYYYYMMDD(stt)=#{date}　group by　keyword　order by　sum(keyword_stats.ct)　limit #{limit}")
    List<KeywordStats> selectKeywordStats(@Param("date") Integer date, @Param("limit") Integer limit);
}
