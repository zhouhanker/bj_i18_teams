package realitime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package realitime.dws.util.KeywordUtil
 * @Author zhaohua.liu
 * @Date 2025/4/20.20:25
 * @description:分词工具类
 */
public class KeywordUtil {
    public static List<String> analyze(String text){
        //准备存储单词的list集合
        ArrayList<String> list = new ArrayList<>();
        //设置字符输入流和智能分词
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        try {
            //获取单个词的lexeme对象
            Lexeme next = null;
            while ((next = ikSegmenter.next())!=null){
                String keyword = next.getLexemeText();
                list.add(keyword);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    public static void main(String[] args) {
        List<String> list = analyze("小米手机京东自营5G版");
        list.forEach(word-> System.out.println(word));
    }
}
