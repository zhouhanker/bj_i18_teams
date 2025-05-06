package com.zgq.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.zgq.bean.TradeProvinceOrderAmount
 * @Author  guoqiang.zhang
 * @Date  2025/5/4 13:41
 * @description: 
*/

@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
 // 省份名称
 String provinceName;
 // 下单金额
 Double orderAmount;
}