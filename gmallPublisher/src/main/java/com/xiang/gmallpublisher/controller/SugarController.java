package com.xiang.gmallpublisher.controller;

import com.xiang.gmallpublisher.beans.KeywordStats;
import com.xiang.gmallpublisher.beans.ProductStats;
import com.xiang.gmallpublisher.beans.ProvinceStats;
import com.xiang.gmallpublisher.beans.VisitorStats;
import com.xiang.gmallpublisher.service.KeywordStatsService;
import com.xiang.gmallpublisher.service.ProductStatsService;
import com.xiang.gmallpublisher.service.ProvinceStatsService;
import com.xiang.gmallpublisher.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/17
 * Desc:
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {
    @Autowired
    private ProductStatsService productStatsService;
    @Autowired
    private ProvinceStatsService provinceStatsService;

    @Autowired
    private VisitorStatsService visitorStatsService;

    @Autowired
    private KeywordStatsService keywordStatsService;

    @RequestMapping("keyword")
    public String getKeyword(
            @RequestParam(value = "date",defaultValue = "0" ) Integer date,
            @RequestParam(value = "limit", defaultValue = "5") Integer limit
    ){
        if(date == 0){
            date = now();
        }
        List<KeywordStats> keywordStatsList = keywordStatsService.getKeywordStats(date, limit);

        StringBuilder jsonSb=new StringBuilder( "{\"status\":0,\"msg\":\"\",\"data\":[" );
        //循环拼接字符串
        for (int i = 0; i < keywordStatsList.size(); i++) {
            KeywordStats keywordStats =  keywordStatsList.get(i);
            if(i>=1){
                jsonSb.append(",");
            }
            jsonSb.append(  "{\"name\":\"" + keywordStats.getKeyword() + "\"," +
                    "\"value\":"+keywordStats.getCt()+"}");
        }
        jsonSb.append(  "]}");
        return  jsonSb.toString();

    }



    @RequestMapping("/visitorTime")
    public String getVisitorTime(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = now();
        }
        List<VisitorStats> visitorTime = visitorStatsService.getVisitorTime(date);
        //构建24位数组
        VisitorStats[] visitorStatsArr=new VisitorStats[24];

        //把对应小时的位置赋值
        for (VisitorStats visitorStats : visitorTime) {
            visitorStatsArr[visitorStats.getHr()] =visitorStats ;
        }
        List<String> hrList=new ArrayList<>();
        List<Long> uvList=new ArrayList<>();
        List<Long> pvList=new ArrayList<>();
        List<Long> newMidList=new ArrayList<>();

        //循环出固定的0-23个小时  从结果map中查询对应的值
        for (int hr = 0; hr <=23 ; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            if (visitorStats!=null){
                uvList.add(visitorStats.getUv_ct())   ;
                pvList.add( visitorStats.getPv_ct());
                newMidList.add( visitorStats.getNew_uv());
            }else{ //该小时没有流量补零
                uvList.add(0L)   ;
                pvList.add( 0L);
                newMidList.add( 0L);
            }
            //小时数不足两位补零
            hrList.add(String.format("%02d", hr));
        }
        //拼接字符串
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\""+StringUtils.join(hrList,"\",\"")+ "\"],\"series\":[" +
                "{\"name\":\"uv\",\"data\":["+ StringUtils.join(uvList,",") +"]}," +
                "{\"name\":\"pv\",\"data\":["+ StringUtils.join(pvList,",") +"]}," +
                "{\"name\":\"新用户\",\"data\":["+ StringUtils.join(newMidList,",") +"]}]}}";
        return  json;

    }

    @RequestMapping("/visitor")
    public String getVisitor(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = now();
        }
        List<VisitorStats> visitorStatsByNew = visitorStatsService.getVisitorStatsByNew(date);
        StringBuffer res = new StringBuffer();
        res.append("{\"status\": 0,\"data\": {\"columns\": [{\"name\": \"类别\",\"id\": \"type\"},{\"name\": \"老用户\",\"id\": \"old\"},{\"name\": \"新用户\",\"id\": \"new\"}],\"rows\": [");
        VisitorStats visitorStatsNew = new VisitorStats();
        VisitorStats visitorStatsOld = new VisitorStats();
        if(visitorStatsByNew.get(0).getIs_new().equals("1")){
            visitorStatsNew = visitorStatsByNew.get(0);
            visitorStatsOld = visitorStatsByNew.get(1);
        }else {
            visitorStatsNew = visitorStatsByNew.get(1);
            visitorStatsOld = visitorStatsByNew.get(0);
        }
        res.append("{\"type\": \"用户数(人)\",");
        res.append("\"old\": "+visitorStatsOld.getUv_ct()+",");
        res.append("\"new\":"+visitorStatsNew.getUv_ct()+"},");

        res.append("{\"type\": \"总访问页面(次)\",");
        res.append("\"old\":"+visitorStatsOld.getPv_ct()+",");
        res.append("\"new\":"+visitorStatsNew.getPv_ct()+"},");

        res.append("{\"type\": \"跳出率(%)\",");
        res.append("\"old\":"+visitorStatsOld.getUjRate()+",");
        res.append("\"new\":"+visitorStatsNew.getUjRate()+"},");

        res.append("{\"type\": \"平均在线时长(秒)\",");
        res.append("\"old\":"+visitorStatsOld.getDurPerSv()+",");
        res.append("\"new\":"+visitorStatsNew.getDurPerSv()+"},");

        res.append("{\"type\": \"平均访问页面数(次)\",");
        res.append("\"old\":"+visitorStatsOld.getPvPerSv()+",");
        res.append("\"new\":"+visitorStatsNew.getPvPerSv()+"}");

        res.append("]}}");
        return res.toString();
    }

    @RequestMapping("/province")
    public String getProvinceOrder(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = now();
        }
        List<ProvinceStats> provinceStats = provinceStatsService.getProvinceStats(date);
        StringBuffer res = new StringBuffer();
        res.append("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0;i<provinceStats.size();i++){
            res.append("{");
            res.append("\"name\": \""+provinceStats.get(i).getProvince_name()+"\",\"value\": "+provinceStats.get(i).getOrder_amount()+"");
            res.append("}");
            if(i< provinceStats.size()-1){
                res.append(",");
            }
        }

        res.append("],\"valueName\": \"交易额\"}}");
        return res.toString();
    }

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date){
        if(date == 0){
            date = now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String res = "{\"status\":0,\"data\":"+gmv+"}";
        return res;
    }

    @RequestMapping("/tm")
    public String getGMVByTm(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "5") Integer limit){
        if (date == 0){
            date = now();
        }
        List<ProductStats> productStatsByTm = productStatsService.getProductStatsByTm(date, limit);
        List<String> categories = new ArrayList<>();
        List<BigDecimal> orderAmount = new ArrayList<>();
        for (ProductStats productStats : productStatsByTm) {
            String tm_name = productStats.getTm_name();
            categories.add(tm_name);
            BigDecimal order_amount = productStats.getOrder_amount();
            orderAmount.add(order_amount);
        }
        String res = "{\"status\": 0,\"data\": {\"categories\": [\""+ StringUtils.join(categories,"\",\"") +"\"],\"series\": [{\"name\": \"商品品牌\",\"data\": ["+StringUtils.join(orderAmount,",")+"]}]}}";
        return res;
    }
    @RequestMapping("/categories")
    public String getGMVByCate(
            @RequestParam(value = "date",defaultValue = "0" ) Integer date,
            @RequestParam(value = "limit", defaultValue = "5") Integer limit
    ){
        if(date == 0){
            date = now();
        }
        StringBuffer res = new StringBuffer();
        res.append("{\"status\": 0,\"data\": [");
        List<ProductStats> productStatsByCate = productStatsService.getProductStatsByCate(date,limit);
        for(int i = 0;i<productStatsByCate.size();i++){
            res.append("{\"name\":\""+productStatsByCate.get(i).getCategory3_name()+"\",\"value\":"+productStatsByCate.get(i).getOrder_amount()+"}");
            if(i<productStatsByCate.size() -1){
                res.append(",");
            }
        }
        res.append("]}");
        return res.toString();
    }

    @RequestMapping("/SPU")
    public String getGMVBySPU(
            @RequestParam(value = "date",defaultValue = "0" ) Integer date,
            @RequestParam(value = "limit", defaultValue = "5") Integer limit
    )
    {
        if(date == 0){
            date = now();
        }
        List<ProductStats> productStatsBySPU = productStatsService.getProductStatsBySPU(date, limit);
        StringBuffer res = new StringBuffer();
        res.append("{\"status\": 0,\"data\": {\"columns\": [{\"name\": \"商品名称\",\"id\": \"name\"},{\"name\": \"交易额\",\"id\": \"amount\"},{\"name\": \"订单数\",\"id\": \"ct\"}],\"rows\": [");

        for (int i = 0; i < productStatsBySPU.size(); i++) {
            ProductStats productStats = productStatsBySPU.get(i);
            res.append("{\"name\": \""+productStats.getSpu_name()+"\",\"amount\": "+productStats.getOrder_amount()+",\"ct\": "+productStats.getOrder_ct()+"}");
            if(i< productStatsBySPU.size() -1){
                res.append(",");
            }
        }
        res.append("]}}");
        return res.toString();
    }

    private Integer now() {
        String res = DateFormatUtils.format(new Date(),"yyyyMMdd");
        return Integer.valueOf(res);
    }
}
