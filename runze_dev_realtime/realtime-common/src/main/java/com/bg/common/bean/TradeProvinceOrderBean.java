package com.bg.common.bean;

/**
 * @Package com.jiao.bean.TradeProvinceOrderBean
 * @Author Chen.Run.ze
 * @Date 2025/4/16 15:23
 * @description: 1
 */
import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Set;
@Data
@AllArgsConstructor
@Builder
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
    Long ts;
    @JSONField(serialize = false)
    String orderDetailId;
    @JSONField(serialize = false)
    Set<String> orderIdSet;
}
