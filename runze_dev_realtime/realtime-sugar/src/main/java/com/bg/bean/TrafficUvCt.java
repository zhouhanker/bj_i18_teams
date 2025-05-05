package com.bg.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.bg.bean.TrafficUvCt
 * @Author Chen.Run.ze
 * @Date 2025/4/17 11:18
 * @description: 实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
