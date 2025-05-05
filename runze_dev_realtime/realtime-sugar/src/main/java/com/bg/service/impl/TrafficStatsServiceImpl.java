package com.bg.service.impl;

import com.bg.bean.TrafficUvCt;
import com.bg.mapper.TrafficStatsMapper;
import com.bg.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Package com.bg.service.impl.TrafficStatsServiceImpl
 * @Author Chen.Run.ze
 * @Date 2025/4/17 11:21
 * @description: 流量域统计service接口实现类
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