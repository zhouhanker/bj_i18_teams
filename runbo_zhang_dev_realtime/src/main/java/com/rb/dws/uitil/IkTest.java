package com.rb.dws.uitil;

/**
 * @Package com.rb.dws.uitil.IkTest
 * @Author runbo.zhang
 * @Date 2025/4/14 15:58
 * @description:
 */
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class IkTest extends ScalarFunction {


    public static  List<String>  ik(String s) {
        Analyzer analyzer = new IKAnalyzer(true);
        TokenStream ts = null;
        List<String> list = new ArrayList<String>();

        try {
            ts = analyzer.tokenStream("myfield", new StringReader(s));
            OffsetAttribute offset = (OffsetAttribute)ts.addAttribute(OffsetAttribute.class);
            CharTermAttribute term = (CharTermAttribute)ts.addAttribute(CharTermAttribute.class);
            TypeAttribute type = (TypeAttribute)ts.addAttribute(TypeAttribute.class);
            ts.reset();

            while(ts.incrementToken()) {
               list.add(term.toString()) ;
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

        }

        return list;
    }
}
