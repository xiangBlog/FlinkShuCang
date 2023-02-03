package com.xiang.gmallpublisher.mapper;

import com.xiang.gmallpublisher.beans.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/17
 * Desc:
 */
public interface VisitorStatsMapper {
    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct,sum(sv_ct) sv_ct,sum(uj_ct) uj_ct, sum(dur_sum) dur_sum from visitor_stats where toYYYYMMDD(stt)=#{date} group by is_new")
    List<VisitorStats> selectVisitorStats(Integer date);

    @Select("select toHour(stt) hr,sum(pv_ct) pv_ct,sum(uv_ct) uv_ct,sum(if(is_new='1',visitor_stats.uv_ct,0)) new_uv from visitor_stats where toYYYYMMDD(stt)=#{date} group by hr")
    List<VisitorStats> selectVisitorTime(Integer date);
}
