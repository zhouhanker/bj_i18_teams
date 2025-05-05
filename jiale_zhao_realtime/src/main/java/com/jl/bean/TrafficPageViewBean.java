package com.jl.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.jl.TrafficPageViewBean
 * @Author jia.le
 * @Date 2025/4/8 8:53
 * @description: TrafficPageViewBean
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 回流用户数
    Long backCt;
    // 独立用户数
    Long uuCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
