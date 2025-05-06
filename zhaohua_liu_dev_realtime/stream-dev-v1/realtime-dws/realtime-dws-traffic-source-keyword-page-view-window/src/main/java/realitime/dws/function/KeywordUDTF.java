package realitime.dws.function;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import realitime.dws.util.KeywordUtil;

/**
 * @Package realitime.dws.function.KeywordUDTF
 * @Author zhaohua.liu
 * @Date 2025/4/20.19:58
 * @description:自定义udtf函数
 */
//声明一个row有多少列
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
//自定义函数
public class KeywordUDTF extends TableFunction<Row> {
    //eval方法,output去类中找eval方法,
    // eval方法可以重载,在sql中根据输入参数不同,选择不同的方法
    public void eval(String str) {
        for (String s : KeywordUtil.analyze(str)) {
            collect(Row.of(s));
        }
    }
}
