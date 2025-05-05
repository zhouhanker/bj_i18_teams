package com.bg.service.impl;

import com.bg.bean.TradeProvinceOrderAmount;
import com.bg.mapper.TradeStatsMapper;
import com.bg.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.bg.service.impl.TradeStatsServiceImpl
 * @Author Chen.Run.ze
 * @Date 2025/4/17 10:45
 * @description: 交易域统计service接口实现类
 */

@Service
public class TradeStatsServiceImpl implements TradeStatsService {
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
