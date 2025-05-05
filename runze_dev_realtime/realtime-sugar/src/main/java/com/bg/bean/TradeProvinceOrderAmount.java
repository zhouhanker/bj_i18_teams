package com.bg.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.bg.bean.TradeProvinceOrderAmount
 * @Author Chen.Run.ze
 * @Date 2025/4/17 11:18
 * @description: 实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
