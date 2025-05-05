package com.lxy.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.realtime.common.bean.CartAddUuBean
 * @Author luoxinyu
 * @Date 2025/4/9 19:30
 * @description: 1
 */
@Data
@NoArgsConstructor
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
