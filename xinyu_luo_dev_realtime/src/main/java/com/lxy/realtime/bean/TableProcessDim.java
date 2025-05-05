package com.lxy.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.realtime.common.bean.TableProcessDim
 * @Author luoxinyu
 * @Date 2025/4/9 19:32
 * @description: 1
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    // sink到 hbase 的时候的主键字段
    String sinkRowKey;

    // 配置表操作类型
    String op;
}
