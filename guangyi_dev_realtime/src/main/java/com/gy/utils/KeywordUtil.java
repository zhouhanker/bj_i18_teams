package com.gy.utils;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Felix
 * @date 2024/6/09
 * 分词工具类
 */
public class KeywordUtil {
    //分词
    public static List<String> get(String str) {
        Analyzer analyzer = new IKAnalyzer(true);
        TokenStream ts = null;
        ArrayList<String> objects = new ArrayList<>();
        try {
            ts = analyzer.tokenStream("myfield", new StringReader(str));
            OffsetAttribute offset = (OffsetAttribute) ts.addAttribute(OffsetAttribute.class);
            CharTermAttribute term = (CharTermAttribute) ts.addAttribute(CharTermAttribute.class);
            TypeAttribute type = (TypeAttribute) ts.addAttribute(TypeAttribute.class);
            ts.reset();

            while (ts.incrementToken()) {
                // System.out.println(term.toString());
                objects.add(term.toString());
            }

            ts.end();
        } catch (IOException var14) {
            var14.printStackTrace();
        } finally {
            if (ts != null) {
                try {
                    ts.close();
                } catch (IOException var13) {
                    var13.printStackTrace();
                }
            }
            return  objects;

        }
    }

}

