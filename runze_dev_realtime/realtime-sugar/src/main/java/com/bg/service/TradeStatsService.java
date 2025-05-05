package com.bg.service;

import com.bg.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.bg.service.TradeStatsService
 * @Author Chen.Run.ze
 * @Date 2025/4/17 10:41
 * @description: 交易域统计Service接口
 */
public interface TradeStatsService {
    //获取某天总交易额
    BigDecimal getGMV(Integer date);
    //获取某天各个省份交易额
    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
