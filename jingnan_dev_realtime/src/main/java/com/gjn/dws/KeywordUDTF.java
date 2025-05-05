package com.gjn.dws;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 *  @Package com.gjn.dws.KeywordUDTF
 *  @Author jingnan.guo
 *  @Date 2025/4/17 11:47
 * @description: UDTF
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String test) {
        for (String keyword : KeywordUtil.analyze(test)){
            collect(Row.of(keyword));
        }
    }


}
