package com.example.flink.realtime.publisher.service.impl;

import com.example.flink.realtime.publisher.bean.TrafficUvCt;
import com.example.flink.realtime.publisher.mapper.TrafficStatsMapper;
import com.example.flink.realtime.publisher.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.service.impl.impl.TrafficStatsServiceImpl
 * @Author liu.bo
 * @Date 2025/5/4 15:06
 * @description:流量域统计service接口实现类
 */
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    @Autowired
    private TrafficStatsMapper trafficStatsMapper;

    @Override
    public List<TrafficUvCt> getChUvCt(Integer date, Integer limit) {
        return trafficStatsMapper.selectChUvCt(date,limit);
    }
}
