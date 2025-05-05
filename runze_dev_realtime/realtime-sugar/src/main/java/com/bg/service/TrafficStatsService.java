package com.bg.service;

import com.bg.bean.TrafficUvCt;

import java.util.List;

/**
 * @Package com.bg.service.TrafficStatsService
 * @Author Chen.Run.ze
 * @Date 2025/4/17 11:20
 * @description: 流量域统计service接口
 */
public interface TrafficStatsService {
    //获取某天各个渠道独立访客数
    List<TrafficUvCt> getChUvCt(Integer date, Integer limit);
}
