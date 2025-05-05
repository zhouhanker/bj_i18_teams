package com.lzr.conf.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.lzr.retail.com.lzy.stream.realtime.v1.realtime.bean.TableProcessDwd
 * @Author lv.zirao
 * @Date 2025/4/8 11:05
 * @description: TableProcessDwd
 */

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
