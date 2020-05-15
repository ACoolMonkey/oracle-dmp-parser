package com.hys.odp.service;

import com.hys.odp.model.AnalyzeTypeEnum;
import com.hys.odp.model.Column;
import com.hys.odp.model.Row;
import com.hys.odp.model.Table;
import com.hys.odp.util.DmpAnalyzeHelper;
import com.hys.odp.util.DmpAnalyzeUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 数据模式分析
 *
 * @author Robert Hou
 * @date 2019年3月21日
 */
@Slf4j
public class RealDataAnalyze implements BaseAnalyze {

    /**
     * 拼接insertSql正则表达式
     */
    private static final Pattern PATTERN = Pattern.compile("(.*VALUES \\().*");

    @Override
    public int analyze(RandomAccessFile raf) throws IOException {
        // 读取 无用字节下的数据
        appendReadParts(raf, DmpAnalyzeHelper.getRowFlag());
        return 0;
    }

    /**
     * 读取真实数据阶段<br>
     * 每存在一个数据行 就调用这个方法
     *
     * @param raf RandomAccessFile
     */
    private int appendReadParts(RandomAccessFile raf, int c) throws IOException {
        // 只有当传入0的时候才创建对象 当传入1时候不创建
        // 程序说明 当遇见空字段的时候 是return 将rowFlag变成1
        // 就不能保证null是同一个数据行对象
        // rowFlag为0是代表同一行 为1时 出现空了 不创建对象
        if (DmpAnalyzeHelper.getRowFlag() == 0) {
            Row r = new Row();
            DmpAnalyzeHelper.setColumnList(r.getCols());
        }
        while (true) {
            long filePointer = raf.getFilePointer();
            DmpAnalyzeHelper.setColumn(new Column());
            // 字段的字节长度
            byte[] columnLength = new byte[2];
            byte firstByte = raf.readByte();
            columnLength[1] = firstByte;
            // 如果第一个字段是空的 或者数据行中包含空的
            // FE FF
            // FF FF 0A
            if (firstByte == (byte) 0xFE) {
                DmpAnalyzeHelper.setRowFlag(1);
                DmpAnalyzeHelper.accumulateDataCountFlag();
                raf.readByte();
                addColumnList(new Column(null));
                return -2;
            }
            // 如果读取到了真实数据的末尾 00 00 FF FF 0A
            if (firstByte == (byte) 0xFF) {
                DmpAnalyzeHelper.setPatternFlag(AnalyzeTypeEnum.UNDATA);
                String readLine = raf.readLine();
                return 0;
            }
            // 如果 最后一个字段为空 -2 -1 0 0 -1 -1 10
            if (firstByte == (byte) 0x00) {
                byte secZerByte = raf.readByte();
                columnLength[0] = secZerByte;
                // 读取到了数据行的末尾
                if (DmpAnalyzeHelper.getDataCountFlag() == DmpAnalyzeHelper.getDataCount()) {
                    List<Row> rowList = DmpAnalyzeHelper.getRowList();
                    rowList.add(DmpAnalyzeHelper.getRow());
                    DmpAnalyzeHelper.setRowList(rowList);
                    DmpAnalyzeHelper.setRowFlag(0);
                    DmpAnalyzeHelper.clearDataCountFlag();
                    return 1;
                }
            } else {
                byte secondByte = raf.readByte();
                columnLength[0] = secondByte;
            }
            // 待更改 将字节数组 改成 int数值
            int columnByteLength = DmpAnalyzeUtils.getInt(columnLength);
            if (columnByteLength == 0) {
                DmpAnalyzeHelper.setPatternFlag(AnalyzeTypeEnum.UNDATA);
                return 0;
            }
            byte[] columnByteArray = new byte[columnByteLength];
            // 读取真实数据行的每一字段
            while (true) {
                DmpAnalyzeHelper.accumulateDataPartFlag();
                System.arraycopy(columnByteArray, 1, columnByteArray, 0, columnByteArray.length - 1);
                byte cur = raf.readByte();
                columnByteArray[columnByteArray.length - 1] = cur;
                // 说明该字段的字节读取完毕
                if (DmpAnalyzeHelper.getDataPartFlag() == columnByteLength) {
                    // 每次读满一个字段的字节 说明读完一个字段
                    String dataTypeName = DmpAnalyzeHelper.getDataTypeList().get(DmpAnalyzeHelper.getDataCountFlag());
                    if (dataTypeName.contains("NUMBER") || dataTypeName.contains("FLOAT") || dataTypeName.contains("DOUBLE")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseNumber(columnByteArray)));
                    } else if (dataTypeName.contains("VARCHAR")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseString(columnByteArray)));
                    } else if (dataTypeName.contains("DATE")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseTimeStamp(columnByteArray)));
                    } else if (dataTypeName.contains("TIMESTAMP")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseTimeStamp(columnByteArray)));
                    } else if (dataTypeName.contains("NVARCHAR")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseString(columnByteArray)));
                    } else if (dataTypeName.contains("BLOB")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseBlob(columnByteArray)));
                    } else if (dataTypeName.contains("CLOB")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseClob(columnByteArray)));
                    } else if (dataTypeName.contains("CHAR")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseString(columnByteArray)));
                    } else if (dataTypeName.contains("DATETIME")) {
                        addColumnList(new Column(DmpAnalyzeUtils.parseTimeStamp(columnByteArray)));
                    } else {
                        throw new UnsupportedOperationException("不识别的类型！需要进行查看！类型：" + dataTypeName);
                    }
                    DmpAnalyzeHelper.accumulateDataCountFlag();
                    DmpAnalyzeHelper.clearDataPartFlag();
                    break;
                }
            }
            // 说明读取的字段数 已经达到一条数据行的数量
            if (DmpAnalyzeHelper.getDataCountFlag() == DmpAnalyzeHelper.getDataCount()) {
                List<Row> rowList = DmpAnalyzeHelper.getRowList();
                rowList.add(DmpAnalyzeHelper.getRow());
                DmpAnalyzeHelper.setRowList(rowList);
                // 拼接insert语句
                jointInsertSql();
                // 0
                raf.readByte();
                // 0
                raf.readByte();
                DmpAnalyzeHelper.setRowFlag(0);
                DmpAnalyzeHelper.clearDataCountFlag();
                return 1;
            }
        }
    }

    /**
     * 拼接当前table中的insert语句
     */
    private void jointInsertSql() {
        Map<String, Table> tableMap = DmpAnalyzeHelper.getTableMap();
        // 获取当前Table
        String index = DmpAnalyzeHelper.getFileCounter() + ":" + DmpAnalyzeHelper.getTableCounter();
        Table table = tableMap.get(index);
        if (table == null) {
            throw new RuntimeException("获取不到当前table！索引：" + index);
        }
        // 获取批处理语句
        String batchInsertIntoSql = table.getInsert_into();
        // 正则拼接
        Matcher m = PATTERN.matcher(batchInsertIntoSql);
        if (m.find()) {
            StringBuilder sb = new StringBuilder();
            sb.append(m.group(1));
            for (Column column : DmpAnalyzeHelper.getColumnList()) {
                if (column.getObject() == null) {
                    sb.append("NULL, ");
                } else {
                    sb.append("'").append(column.getObject()).append("', ");
                }
            }
            sb.replace(sb.length() - 2, sb.length(), ");");
            List<String> insertSql = table.getInsertSql();
            insertSql.add(sb.toString());
            table.setInsertSql(insertSql);
            DmpAnalyzeHelper.setTableMap(index, table);
        }
    }

    /**
     * ColumnList中添加Column
     *
     * @param column 新的Column
     */
    private void addColumnList(Column column) {
        List<Column> columnList = DmpAnalyzeHelper.getColumnList();
        columnList.add(column);
        DmpAnalyzeHelper.setColumnList(columnList);
    }
}
