package com.jl.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Set;

/**
 * @Package com.jl.TradeProvinceOrderBean
 * @Author jia.le
 * @Date 2025/4/8 8:49
 * @description: TradeProvinceOrderBean
 */

@Data
@Builder
@AllArgsConstructor
public class TradeProvinceOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;
    // 省份 ID
    String provinceId;
    // 省份名称
    @Builder.Default
    String provinceName = "";

    // 累计下单次数
    Long orderCount;
    // 累计下单金额
    BigDecimal orderAmount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts_ms;

    @JSONField(serialize = false)
    Set<String> orderIdSet;
}
