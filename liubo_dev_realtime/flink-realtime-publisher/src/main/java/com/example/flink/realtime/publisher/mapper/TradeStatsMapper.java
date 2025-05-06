package com.example.flink.realtime.publisher.mapper;

import com.example.flink.realtime.publisher.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.demo.TradeStatsMapper
 * @Author liu.bo
 * @Date 2025/5/4 14:59
 * @description:
 */
/**
 * {
 * "status": 0,
 * "msg": "",
 * "data": 130217.1810743946
 * }
 */
public interface TradeStatsMapper {
    //获取某天总交易额
    @Select("SELECT SUM(order_amount) order_amount FROM dws_trade_province_order_window PARTITION p${date}")
    BigDecimal selectGMV(Integer date);

    //获取某天各个身份交易额
    @Select("SELECT province_name,SUM(order_amount) order_amount " +
            " FROM dws_trade_province_order_window PARTITION p${date} " +
            " GROUP BY province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);

}
