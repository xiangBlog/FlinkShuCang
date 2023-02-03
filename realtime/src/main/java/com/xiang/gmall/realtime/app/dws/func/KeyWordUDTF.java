package com.xiang.gmall.realtime.app.dws.func;

import com.xiang.gmall.realtime.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * User: 51728
 * Date: 2022/11/16
 * Desc: 自定义UDTF函数
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF extends TableFunction<Row> {

    public void eval(String text){
        List<String> keyWordList = KeyWordUtil.analyze(text);
        for(String keyWord:keyWordList){
            collect(Row.of(keyWord));
        }
    }
}
