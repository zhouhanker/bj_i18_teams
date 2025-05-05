package com.hwq.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
@AllArgsConstructor
/**
 * @Package com.hwq.bean.TableProcessDin
 * @Author hu.wen.qi
 * @Date 2025/5/4
 * @description: 1
 */
public class TableProcessDin {
    //1 来源表名
    String sourceTable;
    // 目标表明
    String sinkTable;
    //输出字段
    String sinkColunns;
    //数据列hbase 的列族
    String sinkFamily;
    //sink列hbase 的时线的主健字段
    String sinkRowKey;
    // 配置表操作类型
    String op;
}
