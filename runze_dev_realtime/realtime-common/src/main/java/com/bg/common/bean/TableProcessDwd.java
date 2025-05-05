package com.bg.common.bean;

/**
 * @Package com.bg.common.bean.TableProcessDwd
 * @Author Chen.Run.ze
 * @Date 2025/4/11 18:34
 * @description: 实体类
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
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

