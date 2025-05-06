package com.dws.realtime_dws_traffic_source_keyword_page_view_window.function;

import com.dws.realtime_dws_traffic_source_keyword_page_view_window.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author Felix
 * @date 2024/6/10
 * 自定义UDTF函数
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
