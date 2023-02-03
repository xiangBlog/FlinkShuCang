package com.xiang.logger.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FirstController {
    @RequestMapping("/first")
    public String first(@RequestParam("haha") String username,
                        @RequestParam("heihei") String password){
        System.out.println(username+":::"+password);
        return "success";
    }
}
