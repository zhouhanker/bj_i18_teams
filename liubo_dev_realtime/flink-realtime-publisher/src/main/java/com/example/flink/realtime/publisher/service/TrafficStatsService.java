package com.example.flink.realtime.publisher.service;

import com.example.flink.realtime.publisher.bean.TrafficUvCt;

import java.util.List;

/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.service.impl.TrafficStatsService
 * @Author liu.bo
 * @Date 2025/5/4 15:01
 * @description:  流量域统计service接口
 */
public interface TrafficStatsService {
    //获取某天各个渠道独立访客数
    List<TrafficUvCt> getChUvCt(Integer date, Integer limit);
}
