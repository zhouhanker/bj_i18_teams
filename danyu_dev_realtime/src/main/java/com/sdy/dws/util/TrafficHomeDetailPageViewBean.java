package com.sdy.dws.util;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.sdy.retail.v1.realtime.dws.util.TrafficHomeDetailPageViewBean
 * @Author danyu-shi
 * @Date 2025/4/15 22:01
 * @description:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficHomeDetailPageViewBean {
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
