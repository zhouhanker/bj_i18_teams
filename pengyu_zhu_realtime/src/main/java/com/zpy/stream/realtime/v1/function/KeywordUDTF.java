package com.zpy.stream.realtime.v1.function;

import com.zpy.stream.realtime.v1.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Package com.zpy.stream.realtime.v1.function.KeywordUDTF
 * @Author pengyu_zhu
 * @Date 2025/4/22 15:27
 * @description: KeywordUDTF
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
