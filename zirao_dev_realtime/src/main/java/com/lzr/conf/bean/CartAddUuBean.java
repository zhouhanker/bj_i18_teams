package com.lzr.conf.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.lzr.retail.com.lzy.stream.realtime.v1.realtime.bean.CartAddUuBean
 * @Author lv.zirao
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
