package com.hys.odp.service;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 基础分析接口
 *
 * @author Robert Hou
 * @date 2019年3月21日
 */
public interface BaseAnalyze {

    /**
     * 分析方法
     *
     * @param raf RandomAccessFile
     * @return 无用数据模式需要返回的标志位，其他模式下该返回值无意义
     * @throws IOException RandomAccessFile抛出的异常
     */
    int analyze(RandomAccessFile raf) throws IOException;
}
