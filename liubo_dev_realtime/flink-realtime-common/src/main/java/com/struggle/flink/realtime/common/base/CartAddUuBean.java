package com.struggle.flink.realtime.common.base;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @version 1.0
 * @ Package com.struggle.flink.realtime.common.base.CartAddUuBean
 * @ Author liu.bo
 * @ Date 2025/5/3 14:07
 * @ description: 加购独立用户
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
