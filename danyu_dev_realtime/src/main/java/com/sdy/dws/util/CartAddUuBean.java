package com.sdy.dws.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Felix
 * @date 2024/6/11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    private String stt;
    // 窗口闭合时间
    private String edt;
    // 当天日期
    private String curDate;
    // 加购独立用户数
    private Long cartAddUuCt;
}
