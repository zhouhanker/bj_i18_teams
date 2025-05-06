package com.flink.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.struggle.flink.realtime.common.bean.TableProcessDim
 * @Author guo.jia.hui
 * @Date 2025/4/8 22:28
 * @description: Dim层封装的类
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
