package com.realtime_dws.function;

import com.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Package realtime_dws.function.KeywordUDTF
 * @Author ayang
 * @Date 2025/4/14 19:41
 * @description: 调用
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.get(text)) {
            collect(Row.of(keyword));
        }
    }
}

