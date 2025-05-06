package com.struggle.flink.realtime.common.base;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ version 1.0
 * @ Package com.struggle.flink.realtime.common.base.UserLoginBean
 * @ Author liu.bo
 * @ Date 2025/5/3 14:06
 * @ description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {
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
