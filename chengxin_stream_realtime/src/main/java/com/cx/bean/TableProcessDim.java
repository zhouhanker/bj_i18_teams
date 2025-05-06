package com.cx.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcessDim {
    String sourceTable;
    String sinkTable;
    String sinkColumns;
    String sinkFamily;
    String sinkRowKey;
    String op;
}
