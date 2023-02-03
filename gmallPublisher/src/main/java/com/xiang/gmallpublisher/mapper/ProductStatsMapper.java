package com.xiang.gmallpublisher.mapper;

import com.xiang.gmallpublisher.beans.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

// 商品统计Mapper接口
public interface ProductStatsMapper {
    // 获取某天商品总交易额
    @Select(" select sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt)=#{date} ")
    BigDecimal selectGMV(Integer date);

    @Select("select tm_id,tm_name,sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsByTm(@Param("date") Integer date, @Param("limit") Integer limit);

    @Select("select category3_id,category3_name,sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsByCate(@Param("date") Integer date,@Param("limit")Integer limit);

    @Select("select sum(order_ct) order_ct,spu_id,spu_name,sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsBySpu(@Param("date") Integer date,@Param("limit")Integer limit);
}
