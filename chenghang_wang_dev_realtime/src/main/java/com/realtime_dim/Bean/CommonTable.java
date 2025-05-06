package com.realtime_dim.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package realtime_Dim.bean.CommonTable
 * @Author ayang
 * @Date 2025/4/8 21:56
 * @description: 公共实体类
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class CommonTable {
    // 来源表名
    String sourcetable;

    // 目标表名
    String sinktable;

    // 输出字段
    String sinkcolumns;

    // 数据到 hbase 的列族
    String sinkfamily;

    // sink到 hbase 的时候的主键字段
    String sinkfowKey;

    // 配置表操作类型
    String op;
}
