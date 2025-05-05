package com.sdy.dws.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.sdy.retail.v1.realtime.dws.util.UserRegisterBean
 * @Author danyu-shi
 * @Date 2025/4/16 15:53
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    private String stt;
    // 窗口终止时间
    private String edt;
    // 当天日期
    private String curDate;
    // 注册用户数
    private Long registerCt;
}
