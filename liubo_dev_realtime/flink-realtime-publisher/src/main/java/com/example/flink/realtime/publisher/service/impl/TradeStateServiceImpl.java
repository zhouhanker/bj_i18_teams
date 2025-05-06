package com.example.flink.realtime.publisher.service.impl;

import com.example.flink.realtime.publisher.bean.TradeProvinceOrderAmount;
import com.example.flink.realtime.publisher.mapper.TradeStatsMapper;
import com.example.flink.realtime.publisher.service.TradeStateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.service.impl.impl.TradeStateServiceImpl
 * @Author liu.bo
 * @Date 2025/5/4 15:06
 * @description:交易域统计Service接口实现类
 */
@Service
public class TradeStateServiceImpl implements TradeStateService {
    @Autowired
    private TradeStatsMapper tradeStatsMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
