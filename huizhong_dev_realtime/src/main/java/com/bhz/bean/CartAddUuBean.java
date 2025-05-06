package com.bhz.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.bhz.bean.CartAddUuBean
 * @Author huizhong.bai
 * @Date 2025/5/2 14:36
 * @description: CartAddUuBean
 */

@Data
@AllArgsConstructor
public class CartAddUuBean {
    String stt;
    String edt;
    String curDate;
    Long cartAddUuCt;
}
