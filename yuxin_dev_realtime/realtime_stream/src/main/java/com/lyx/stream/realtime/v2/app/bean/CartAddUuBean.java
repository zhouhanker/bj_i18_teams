package com.lyx.stream.realtime.v2.app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.lyx.retail.v1.realtime.bean.CartAddUuBean
 * @Author zhengwei_zhou
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
