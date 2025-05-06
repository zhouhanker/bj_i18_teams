package flink.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 * @Package flink.realtime.dws.util.KeywordUtil
 * @Author liu.bo
 * @Date 2025/5/4 14:40
 * @description:  IK分词器
 */
public class KeywordUtil {
    //分词
    public static List<String> analyze(String text) {
        List<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(reader, true);
        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next()) != null) {
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return keywordList;
    }

    public static void main(String[] args) {
        List<String> str = analyze("小米手机京东自营");
        System.out.println(str);
    }
}
