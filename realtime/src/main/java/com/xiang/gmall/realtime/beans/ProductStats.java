package com.xiang.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * User: 51728
 * Date: 2022/11/15
 * Desc:
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductStats {
    String stt;//窗口起始时间
    String edt;  //窗口结束时间
    Long sku_id; //sku编号
    String sku_name;//sku名称
    BigDecimal sku_price; //sku单价
    Long spu_id; //spu编号
    String spu_name;//spu名称
    Long tm_id; //品牌编号
    String tm_name;//品牌名称
    Long category3_id;//品类编号
    String category3_name;//品类名称


    @lombok.Builder.Default
    Long display_ct = 0L; //曝光数

    @lombok.Builder.Default
    Long click_ct = 0L;  //点击数

    @lombok.Builder.Default
    Long favor_ct = 0L; //收藏数

    @lombok.Builder.Default
    Long cart_ct = 0L;  //添加购物车数

    @lombok.Builder.Default
    Long order_sku_num = 0L; //下单商品个数

    @lombok.Builder.Default   //下单商品金额
    BigDecimal order_amount = BigDecimal.ZERO;

    @lombok.Builder.Default
    Long order_ct = 0L; //订单数

    @lombok.Builder.Default   //支付金额
    BigDecimal payment_amount = BigDecimal.ZERO;

    @lombok.Builder.Default
    Long paid_order_ct = 0L;  //支付订单数

    @lombok.Builder.Default
    Long refund_order_ct = 0L; //退款订单数

    @lombok.Builder.Default
    BigDecimal refund_amount = BigDecimal.ZERO;

    @lombok.Builder.Default
    Long comment_ct = 0L;//评论数

    @lombok.Builder.Default
    Long good_comment_ct = 0L; //好评数

    @lombok.Builder.Default
    @TransientSink
    Set orderIdSet = new HashSet();  //用于统计订单数

    @lombok.Builder.Default
    @TransientSink
    Set paidOrderIdSet = new HashSet(); //用于统计支付订单数

    @lombok.Builder.Default
    @TransientSink
    Set refundOrderIdSet = new HashSet();//

    Long ts; //统计时间戳

}
