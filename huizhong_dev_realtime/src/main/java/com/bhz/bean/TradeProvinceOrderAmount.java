package com.bhz.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.bhz.bean.TradeProvinceOrderAmount
 * @Author huizhong.bai
 * @Date 2025/5/2 14:36
 * @description: TradeProvinceOrderAmount
 */

@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
