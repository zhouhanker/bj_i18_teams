package com.common.Bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TableProcessDim {
    //来源表明
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的判族
    String sinkFamily;

    // sink列 hbase 的时候的主键宁段
    String sinkRowKey;

    //配置表保作炎型
    String op;


}
