package dws.function;



import util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
/**
 * @Package com.cm.dws.function.KeywordUDTF
 * @Author chen.ming
 * @Date 2025/4/14 10:59
 * @description: 自定义函数
 */
@FunctionHint(output =@DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
       public void  eval(String text){
           for (String keyword : KeywordUtil.analyze(text)){
               collect(Row.of(keyword));
           }
       }
}
