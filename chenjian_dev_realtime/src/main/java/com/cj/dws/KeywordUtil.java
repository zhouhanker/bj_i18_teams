package com.cj.dws;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package com.cj.realtime.dws.KeywordUtil
 * @Author chen.jian
 * @Date 2025/4/14 11:02
 * @description: 分词器
 */
public class KeywordUtil {
    public static List<String> analyze(String test){
        ArrayList<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(test);
        IKSegmenter ik = new IKSegmenter(reader, false);
        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next()) != null) {
                String lexemeText = lexeme.getLexemeText();
                keywordList.add(lexemeText);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return keywordList;
    }

    public static void main(String[] args) {
        System.out.println(analyze("小米手机京东自营5G移动联通电信"));
    }
}
