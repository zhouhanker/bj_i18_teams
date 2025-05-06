package com.dpk.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.lzy.retail.v1.realtime.bean.CartAddUuBean
 * @Author pengkun_du
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
