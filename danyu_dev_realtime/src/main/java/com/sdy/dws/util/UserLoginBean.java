package com.sdy.dws.util;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Felix
 * @date 2024/6/11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    private  String stt;
    // 窗口终止时间
    private String edt;
    // 当天日期
    private String curDate;
    // 回流用户数
    private Long backCt;
    // 独立用户数
    private  Long uuCt;
    // 时间戳
    @JSONField(serialize = false)
    private Long ts;
}

