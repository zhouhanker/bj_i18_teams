package com.flink.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.struggle.flink.realtime.common.bean.TrafficPageViewBean
 * @Author guo.jia.hui
 * @Date 2025/4/18 11:40
 * @description: 实体类对象
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TrafficPageViewBean {
    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String cur_date;
    // app版本号
    private String vc;
    // 渠道
    private String ch;
    // 地区
    private String ar;
    // 新老访客状态标记
    private String isNew;

    // 独立访客数
    private Long uvCt;
    // 页面浏览数
    private Long pvCt;
    // 累计访问时长
    private Long durSum;
    // 时间戳
    @JSONField(serialize = false) //不序列化这个字段
    private Long ts;
}
