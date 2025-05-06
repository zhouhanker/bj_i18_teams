package com.ytx.realtime.dws.sourcekey.function;


import com.ytx.realtime.dws.sourcekey.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF  extends TableFunction<Row>{
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
