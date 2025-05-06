package flink.realtime.dws.function;

import flink.realtime.dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @version 1.0
 * @Package flink.realtime.dws.function.KeywordUDTF
 * @Author liu.bo
 * @Date 2025/5/4 14:40
 * @description:  flinksql的一对多函数
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
