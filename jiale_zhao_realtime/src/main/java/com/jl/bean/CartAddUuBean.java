package com.jl.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.jl.CartAddUuBean
 * @Author jia.le
 * @Date 2025/4/8 8:44
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
