package com.bhz.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.bhz.bean.TrafficUvCt
 * @Author huizhong.bai
 * @Date 2025/5/2 14:36
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
