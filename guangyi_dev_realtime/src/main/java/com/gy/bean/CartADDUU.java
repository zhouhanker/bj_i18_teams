package com.gy.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package bean.CartADDUU
 * @Author guangyi_zhou
 * @Date 2025/4/16 11:38
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class CartADDUU {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
