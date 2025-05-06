package com.lzr.conf.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.lzr.retail.com.lzy.realtime.bean.TableProcessDim
 * @Author lv.zirao
 * @Date 2025/4/8 8:47
 * @description: TableProcessDim
 */

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
