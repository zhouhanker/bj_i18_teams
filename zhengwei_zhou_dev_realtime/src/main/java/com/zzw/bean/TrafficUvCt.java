package com.zzw.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.lzy.retail.v1.realtime.bean.TrafficUvCt
 * @Author zhengwei_zhou
 * @Date 2025/4/8 13:56
 * @description: TrafficUvCt
 */

@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
