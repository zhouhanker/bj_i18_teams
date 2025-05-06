package com.example.flink.realtime.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.bean.TradeProvinceOrderAmount
 * @Author liu.bo
 * @Date 2025/5/4 14:56
 * @description: 各省份交易额
 */
@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 各省份交易总额
    Double orderAmount;
}
