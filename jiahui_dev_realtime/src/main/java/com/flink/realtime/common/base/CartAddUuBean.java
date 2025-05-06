package com.flink.realtime.common.base;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.struggle.flink.realtime.common.base.CartAddUuBean
 * @Author guo.jia.hui
 * @Date 2025/4/20 20:59
 * @description: 加购独立用户
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
