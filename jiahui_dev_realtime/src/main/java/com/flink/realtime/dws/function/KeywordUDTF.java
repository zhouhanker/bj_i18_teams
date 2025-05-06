package com.flink.realtime.dws.function;

import com.flink.realtime.dws.util.KeywordUtil;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Package flink.realtime.dws.function.a
 * @Author guo.jia.hui
 * @Date 2025/4/16 20:33
 * @description: flinksql的一对多函数
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String str) {
        for (String s : KeywordUtil.analyze(str)) {
            // use collect(...) to emit a row
            collect(Row.of(s));
        }
    }
}
