package com.example.flink.realtime.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.bean.TrafficUvCt
 * @Author liu.bo
 * @Date 2025/5/4 14:57
 * @description:
 */
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
