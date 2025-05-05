package com.lzr.conf.function;

import com.lzr.conf.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Package com.lzr.stream.realtime.com.lzy.stream.realtime.v1.function.KeywordUDTF
 * @Author lv.zirao
 * @Date 2025/4/18 18:57
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