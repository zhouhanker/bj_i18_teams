package com.bg.realtime_dws.function;


import com.bg.realtime_dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * @Package com.bg.realtime_dws.function.KwSplit
 * @Author Chen.Run.ze
 * @Date 2025/4/14 11:24
 * @description: 分词函数
 */
@FunctionHint(output = @DataTypeHint("row<keyword string>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String kw) {
        if (kw == null) {
            return;
        }
        // "华为手机白色手机"
        Set<String> keywords = KeywordUtil.split(kw);
        for (String keyword : keywords) {
            collect(Row.of(keyword));
        }
    }
}
