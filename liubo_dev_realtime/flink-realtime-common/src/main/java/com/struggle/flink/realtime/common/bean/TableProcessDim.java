package com.struggle.flink.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ version 1.0
 * @ Package com.struggle.flink.realtime.common.bean.TableProcessDim
 * @ Author liu.bo
 * @ Date 2025/5/3 14:10
 * @ description: DIM层封装的类
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
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
