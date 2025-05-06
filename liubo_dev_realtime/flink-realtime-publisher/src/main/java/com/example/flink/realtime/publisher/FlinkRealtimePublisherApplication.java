package com.example.flink.realtime.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @ version 1.0
 * @ Package com.example.flink.realtime.publisher.FlinkRealtimePublisherApplication
 * @ Author liu.bo
 * @ Date 2025/5/4 14:56
 * @ description:
 */

@SpringBootApplication
@MapperScan(basePackages = "com.example.flink.realtime.publisher.mapper")
public class FlinkRealtimePublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(FlinkRealtimePublisherApplication.class, args);
    }
}
