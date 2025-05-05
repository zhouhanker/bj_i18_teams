package com.gjn.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.gjn.base.TableProcessDim
 * @Author jingnan.guo
 * @Date 2025/4/9 15:03
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcessDim {
    String sourceTable;

    String sinkTable;


    String sinkColumns;


    String sinkFamily;

    String sinkRowKey;


    String op;
}
