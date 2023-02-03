package com.xiang.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/16
 * Desc: 使用IK分词器进行分词
 */
public class KeyWordUtil {
    public static List<String> analyze(String text){
        StringReader stringReader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        List<String> resList = new ArrayList<>();
        try {
            Lexeme lexeme = null;
            while ((lexeme = ikSegmenter.next()) != null){
                resList.add(lexeme.getLexemeText());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return resList;
    }

    public static void main(String[] args) {
        System.out.println(analyze("我说 我是你的爸爸哎"));
    }
}
