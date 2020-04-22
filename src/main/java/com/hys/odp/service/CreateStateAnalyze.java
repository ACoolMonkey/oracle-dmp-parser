package com.hys.odp.service;

import com.hys.odp.model.Table;
import com.hys.odp.util.DmpAnalyzeHelper;
import com.hys.odp.util.DmpAnalyzeUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 表创建模式分析
 *
 * @author Robert Hou
 * @date 2019年3月21日
 */
@Slf4j
public class CreateStateAnalyze implements BaseAnalyze {

    @Override
    public int analyze(RandomAccessFile raf) throws IOException {
        DmpAnalyzeHelper.accumulateTableCounter();
        if (log.isDebugEnabled()) {
            log.debug("第" + DmpAnalyzeHelper.getFileCounter() + "个文件 第" + DmpAnalyzeHelper.getTableCounter() + "张表");
        }
        // tempUnData 表声明 判断表声明是否为空 如果为空 证明是dmp文件的第一个表 读取readLine
        // 如果不为空 那么一定在 无用模式中 赋值为下一张表的表声明
        long filePointer = raf.getFilePointer();
        if (DmpAnalyzeHelper.getTableState() == null) {
            DmpAnalyzeHelper.setTableState(raf.readLine());
        }
        String tableState = DmpAnalyzeHelper.getTableState();
        if (DmpAnalyzeHelper.getTableCreateState() == null) {
            DmpAnalyzeHelper.setTableCreateState(raf.readLine() + ";");
        }
        String tableCreateState = DmpAnalyzeHelper.getTableCreateState();
        String batchInsertSql = raf.readLine();
        if (log.isDebugEnabled()) {
            log.debug("表创建语句：" + DmpAnalyzeHelper.getTableCreateState());
            log.debug("批处理语句：" + batchInsertSql);
        }
        DmpAnalyzeHelper.setTable(
                new Table(DmpAnalyzeHelper.getTableState(), DmpAnalyzeHelper.getTableCreateState(), batchInsertSql));
        DmpAnalyzeHelper.setRowList(DmpAnalyzeHelper.getTable().getRows());
        DmpAnalyzeHelper.setDataTypeList(DmpAnalyzeHelper.getTable().getDataTypeList());
        // 获取数据类型的集合
        DmpAnalyzeHelper.setDataCount(DmpAnalyzeHelper.getDataTypeList().size());
        DmpAnalyzeHelper.clearDataCountFlag();
        // 当前table索引（文件索引+表索引）
        String index = String.valueOf(DmpAnalyzeHelper.getFileCounter() + ":" + DmpAnalyzeHelper.getTableCounter());
        DmpAnalyzeHelper.setTableMap(index, DmpAnalyzeHelper.getTable());
        // 读取至创建表接下来的无用字节 没有用扔掉 不接收
        DmpAnalyzeUtils.readAndJudge(raf, DmpAnalyzeHelper.CREATESTATE_REALDATA);
        return 0;
    }
}
