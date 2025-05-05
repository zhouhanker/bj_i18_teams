package com.gjn.base;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.gjn.base.CartAddUuBean
 * @Author jingnan.guo
 * @Date 2025/4/15 11:04
 * @description: 1
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