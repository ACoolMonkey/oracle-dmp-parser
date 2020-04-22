package com.hys.odp.util;

import com.hys.odp.model.AnalyzeTypeEnum;
import com.hys.odp.model.Column;
import com.hys.odp.model.Row;
import com.hys.odp.model.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle dump文件解析助手类
 *
 * @author Robert Hou
 * @date 2019年3月21日
 */
public class DmpAnalyzeHelper {

    private DmpAnalyzeHelper() {
    }

    /**
     * 真实数据和无用数据之间的间隔
     */
    public static final byte[] REALDATA_UNDATA = new byte[]{0x00, 0x00, (byte) 0xFF, (byte) 0xFF, 0x0A};
    /**
     * 临时真实数据和无用数据之间的间隔
     */
    public static final byte[] REALDATA_UNDATA2 = new byte[]{0x00, 0x00, (byte) 0x3F, (byte) 0x3F, 0x0A};
    /**
     * 创建语句和真实数据之间的间隔
     */
    public static final byte[] CREATESTATE_REALDATA = new byte[]{0x00, 0x00, 0x00, 0x00, 0x00};
    /**
     * 模式切换字段：表创建模式、数据模式、无用模式
     */
    private static final ThreadLocal<AnalyzeTypeEnum> PATTERN_FLAG = ThreadLocal.withInitial(() -> AnalyzeTypeEnum.CREATESTATE);
    /**
     * 表声明语句
     */
    private static final ThreadLocal<String> TABLE_STATE = new ThreadLocal<>();
    /**
     * 表创建语句
     */
    private static final ThreadLocal<String> TABLE_CREATE_STATE = new ThreadLocal<>();
    /**
     * 当前分析文件数量计数器
     */
    private static final ThreadLocal<Integer> FILE_COUNTER = ThreadLocal.withInitial(() -> 1);
    /**
     * 当前分析表数量计数器
     */
    private static final ThreadLocal<Integer> TABLE_COUNTER = ThreadLocal.withInitial(() -> 0);
    /**
     * 字段与字段之间的标志
     */
    private static final ThreadLocal<Integer> DATA_PART_FLAG = ThreadLocal.withInitial(() -> 0);
    /**
     * 一个数据行中的字段数 根据create语句
     */
    private static final ThreadLocal<Integer> DATA_COUNT = ThreadLocal.withInitial(() -> 0);
    /**
     * 不断增加的字段数
     */
    private static final ThreadLocal<Integer> DATA_COUNT_FLAG = ThreadLocal.withInitial(() -> 0);
    /**
     * 存储数据类型
     */
    private static final ThreadLocal<List<String>> DATATYPE_LIST = ThreadLocal.withInitial(ArrayList::new);
    /**
     * 当前字段的空标志
     */
    private static final ThreadLocal<Integer> ROW_FLAG = ThreadLocal.withInitial(() -> 0);
    /**
     * 最后返回的数据，键为当前索引位置（文件索引+表索引），值为解析结果
     */
    private static final ThreadLocal<Map<String, Table>> TABLE_MAP = ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<Table> TABLE = new ThreadLocal<>();
    private static final ThreadLocal<List<Row>> ROW_LIST = new ThreadLocal<>();
    private static final ThreadLocal<Row> ROW = new ThreadLocal<>();
    private static final ThreadLocal<List<Column>> COLUMN_LIST = new ThreadLocal<>();
    private static final ThreadLocal<Column> COLUMN = new ThreadLocal<>();

    /**
     * 清空ThreadLocal值，防止内存泄漏（除了TABLE_MAP和FILE_COUNTER）
     * <pre>
     * 1.TABLE_MAP变量值程序运行到最后再清空,调用removeTableMap方法即可
     * 2.FILE_COUNTER变量值程序运行到最后再清空,调用removeFileCounter方法即可</pre>
     */
    public static void remove() {
        PATTERN_FLAG.remove();
        TABLE_STATE.remove();
        TABLE_CREATE_STATE.remove();
        TABLE_COUNTER.remove();
        DATA_PART_FLAG.remove();
        DATA_COUNT.remove();
        DATA_COUNT_FLAG.remove();
        DATATYPE_LIST.remove();
        ROW_FLAG.remove();
        TABLE.remove();
        ROW_LIST.remove();
        ROW.remove();
        COLUMN_LIST.remove();
        COLUMN.remove();
    }

    public static AnalyzeTypeEnum getPatternFlag() {
        return PATTERN_FLAG.get();
    }

    public static void setPatternFlag(AnalyzeTypeEnum newPatternFlag) {
        PATTERN_FLAG.set(newPatternFlag);
    }

    public static String getTableState() {
        return TABLE_STATE.get();
    }

    public static void setTableState(String newTableState) {
        TABLE_STATE.set(newTableState);
    }

    public static String getTableCreateState() {
        return TABLE_CREATE_STATE.get();
    }

    public static void setTableCreateState(String newTableCreateState) {
        TABLE_CREATE_STATE.set(newTableCreateState);
    }

    public static int getFileCounter() {
        return FILE_COUNTER.get();
    }

    /**
     * TABLE_COUNTER变量值+1
     */
    public static void accumulateFileCounter() {
        FILE_COUNTER.set(FILE_COUNTER.get() + 1);
    }

    /**
     * FILE_COUNTER变量值remove
     */
    public static void removeFileCounter() {
        FILE_COUNTER.remove();
    }

    public static int getTableCounter() {
        return TABLE_COUNTER.get();
    }

    /**
     * TABLE_COUNTER变量值+1
     */
    public static void accumulateTableCounter() {
        TABLE_COUNTER.set(TABLE_COUNTER.get() + 1);
    }

    public static int getDataPartFlag() {
        return DATA_PART_FLAG.get();
    }

    /**
     * DATA_PART_FLAG变量值+1
     */
    public static void accumulateDataPartFlag() {
        DATA_PART_FLAG.set(DATA_PART_FLAG.get() + 1);
    }

    /**
     * DATA_PART_FLAG变量值归零
     */
    public static void clearDataPartFlag() {
        DATA_PART_FLAG.set(0);
    }

    public static int getDataCount() {
        return DATA_COUNT.get();
    }

    public static void setDataCount(int newDataCount) {
        DATA_COUNT.set(newDataCount);
    }

    public static int getDataCountFlag() {
        return DATA_COUNT_FLAG.get();
    }

    /**
     * DATA_COUNT_FLAG变量值+1
     */
    public static void accumulateDataCountFlag() {
        DATA_COUNT_FLAG.set(DATA_COUNT_FLAG.get() + 1);
    }

    /**
     * DATA_COUNT_FLAG变量值归零
     */
    public static void clearDataCountFlag() {
        DATA_COUNT_FLAG.set(0);
    }

    public static List<String> getDataTypeList() {
        return DATATYPE_LIST.get();
    }

    public static void setDataTypeList(List<String> newDataTypeList) {
        DATATYPE_LIST.set(newDataTypeList);
    }

    public static int getRowFlag() {
        return ROW_FLAG.get();
    }

    public static void setRowFlag(int newRowFlag) {
        ROW_FLAG.set(newRowFlag);
    }

    public static Map<String, Table> getTableMap() {
        return TABLE_MAP.get();
    }

    public static void setTableMap(String key, Table value) {
        Map<String, Table> map = TABLE_MAP.get();
        map.put(key, value);
        TABLE_MAP.set(map);
    }

    /**
     * TABLE_LIST变量值remove
     */
    public static void removeTableMap() {
        TABLE_MAP.remove();
    }

    public static Table getTable() {
        return TABLE.get();
    }

    public static void setTable(Table newTable) {
        TABLE.set(newTable);
    }

    public static List<Row> getRowList() {
        return ROW_LIST.get();
    }

    public static void setRowList(List<Row> newRowList) {
        ROW_LIST.set(newRowList);
    }

    public static Row getRow() {
        return ROW.get();
    }

    public static void setRow(Row newRow) {
        ROW.set(newRow);
    }

    public static List<Column> getColumnList() {
        return COLUMN_LIST.get();
    }

    public static void setColumnList(List<Column> newColumnList) {
        COLUMN_LIST.set(newColumnList);
    }

    public static Column getColumn() {
        return COLUMN.get();
    }

    public static void setColumn(Column newColumn) {
        COLUMN.set(newColumn);
    }
}
