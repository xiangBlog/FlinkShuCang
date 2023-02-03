package com.xiang.gmallpublisher.mapper;

import com.xiang.gmallpublisher.beans.ProductStats;
import com.xiang.gmallpublisher.beans.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ProvinceStatsMapper {
    @Select("select sum(order_amount) order_amount ,province_id,province_name from province_stats where toYYYYMMDD(stt)=#{date} group by province_id,province_name")
    List<ProvinceStats> selectProvinceOrderAmount(Integer date);
}
