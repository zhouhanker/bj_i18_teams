package com.realtime_dim.Bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TableProcessDim {
    //来源表明
    String source_table;

    // 目标表名
    String sink_table;

    // 输出字段
    String sink_family;

    // 数据到 hbase 的判族
    String sink_columns;

    // sink列 hbase 的时候的主键宁段
    String sink_row_key;

    //配置表保作炎型
    String op;


}
