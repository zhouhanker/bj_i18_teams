package com.zzw.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.lzy.retail.v1.realtime.bean.TradeProvinceOrderAmount
 * @Author zhengwei_zhou
 * @Date 2025/4/8 11:19
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
