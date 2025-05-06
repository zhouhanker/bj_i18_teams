package com.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package realtime_Dim.bean.TableProcessDwd
 * @Author ayang
 * @Date 2025/4/14 14:40
 * @description: 配置表
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcessDwd {
    // 来源表名
    String sourceTable;

    // 来源类型
    String sourceType;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}
