package com.slh.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.dpk.retail.v1.realtime.bean.CartAddUuBean
 * @Author lihao.song
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
