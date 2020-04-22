package com.hys.odp.service;

import com.hys.odp.model.AnalyzeTypeEnum;
import com.hys.odp.util.DmpAnalyzeHelper;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 无用数据模式分析
 *
 * @author Robert Hou
 * @date 2019年3月21日
 */
public class UnDataAnalyze implements BaseAnalyze {

    @Override
    public int analyze(RandomAccessFile raf) throws IOException {
        // 无用的数据 采用readline读取 读取同时判断 table “
        String tempReadLine = raf.readLine();
        return judgeStringHead(raf, tempReadLine);
    }

    private int judgeStringHead(RandomAccessFile raf, String stringReadLine) throws IOException {
        int flag = -1;
        // 读取无用数据时候 同时读取两行数据 第一个开头是table “ 结尾是” 第二个开头是create
        if (stringReadLine.startsWith("TABLE \"") && stringReadLine.endsWith("\"")) {
            DmpAnalyzeHelper.setTableCreateState(raf.readLine() + ";");
            if (DmpAnalyzeHelper.getTableCreateState().startsWith("CREATE TABLE \"")) {
                DmpAnalyzeHelper.setTableState(stringReadLine);
                flag = 0;
                DmpAnalyzeHelper.setPatternFlag(AnalyzeTypeEnum.CREATESTATE);
                // 切换无用数据时候 已将dataFlag变量赋值为0 接下来就是读取表的模式 完全不必重新赋值 但是也可以
            }
        }
        if ("EXIT".equals(stringReadLine)) {
            long filePointer = raf.getFilePointer();
            byte readByte = raf.readByte();
            // 读取无用模式时候 读到了 EXIT 证明文件已经读取完毕 将flag赋值1
            flag = 1;
        }
        return flag;
    }
}
