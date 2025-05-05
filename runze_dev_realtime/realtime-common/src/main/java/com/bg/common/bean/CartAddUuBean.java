package com.bg.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.bg.common.bean.CartAddUuBean
 * @Author Chen.Run.ze
 * @Date 2025/4/16 8:37
 * @description: 实体类
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
