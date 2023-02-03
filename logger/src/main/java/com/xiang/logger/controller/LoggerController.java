package com.xiang.logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


/***
 * Desc: 日志数据采集
 */
@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate kafkaTemplate;
    @RequestMapping("applog")
    public String log(@RequestParam("param") String logStr){
        //1.打印到输出控制台
//        System.out.println(logStr);
        //2.落盘
        log.info(logStr);
        //3.发送到kafka主题中
        kafkaTemplate.send("ods_base_log",logStr);
        return "success";
    }
}
