package com.example.flink.realtime.publisher.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.demo.HelloController
 * @Author liu.bo
 * @Date 2025/5/4 14:59
 * @description:  test
 */
@RestController
public class HelloController {
    @GetMapping("/hello")
    public String sayHello() {
        return "🎉 Spring Boot 启动成功！访问时间: " + java.time.LocalTime.now();
    }
}
