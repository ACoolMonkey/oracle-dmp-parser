package com.hys.odp;

import com.hys.odp.model.AnalyzeReturnType;
import com.hys.odp.model.AnalyzeTypeEnum;
import com.hys.odp.model.Table;
import com.hys.odp.service.CreateStateAnalyze;
import com.hys.odp.service.RealDataAnalyze;
import com.hys.odp.service.UnDataAnalyze;
import com.hys.odp.util.DmpAnalyzeHelper;
import com.hys.odp.util.DmpAnalyzeUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Oracle dmp文件解析
 *
 * <pre>
 * 3种模式
 *      数据模式 realData-Pattern
 *      表创建模式 createState-Pattern
 *      无用数据模式 unData-Pattern
 * </pre>
 *
 * @author Robert Hou
 * @date 2019年2月15日
 */
@Slf4j
public class AnalyzeDmp {

    private static final String PATH = "/path.properties";

    @SneakyThrows
    public AnalyzeDmp(String path) {
        @Cleanup RandomAccessFile raf = null;
        raf = new RandomAccessFile(new File(path), "r");
        // 跳过固定文件头
        int flag1 = 0x90 * 0x10 + 0xD;
        raf.seek(flag1);
        long filePointer = raf.getFilePointer();
        while (true) {
            if (DmpAnalyzeHelper.getPatternFlag() == AnalyzeTypeEnum.CREATESTATE) {
                new CreateStateAnalyze().analyze(raf);
            } else if (DmpAnalyzeHelper.getPatternFlag() == AnalyzeTypeEnum.REALDATA) {
                new RealDataAnalyze().analyze(raf);
            } else if (DmpAnalyzeHelper.getPatternFlag() == AnalyzeTypeEnum.UNDATA) {
                int breakFlag = new UnDataAnalyze().analyze(raf);
                if (breakFlag == 1) {
                    break;
                }
            }
        }
        DmpAnalyzeHelper.remove();
        // 文件计数+1
        DmpAnalyzeHelper.accumulateFileCounter();
    }

    /**
     * 分析入口
     *
     * @param path 解析文件全路径，包括文件名。该方法只能解析一个文件
     * @return 解析结果
     */
    public static AnalyzeReturnType analyze(String path) {
        log.debug("<<--------------------解析开始-------------------->>");
        // 开始解析
        new AnalyzeDmp(path);
        log.debug("<<--------------------解析结束-------------------->>");
        return afterAnalyze();
    }

    /**
     * 分析入口（读取指定路径下的所有dmp文件）
     *
     * @return 解析结果
     */
    public static AnalyzeReturnType analyze() {
        log.debug("<<--------------------解析开始-------------------->>");
        // 读取文件路径
        Properties p;
        try (FileInputStream in = new FileInputStream(PATH)) {
            p = new Properties();
            p.load(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String path = p.getProperty("path");
        File f = new File(path);
        File[] list = f.listFiles();
        for (File file : list) {
            String filePath = file.getPath();
            // 如果不是dmp文件，则跳过解析
            String substring = filePath.substring(filePath.lastIndexOf(".") + 1);
            if (!"dmp".equalsIgnoreCase(substring)) {
                continue;
            }
            // 开始解析
            new AnalyzeDmp(file.getPath());
        }
        log.debug("<<--------------------解析结束-------------------->>");
        return afterAnalyze();
    }

    /**
     * 解析结束后收尾工作
     *
     * @return 解析结果
     */
    private static AnalyzeReturnType afterAnalyze() {
        DmpAnalyzeHelper.removeFileCounter();
        List<Table> valuesList = new ArrayList<>(DmpAnalyzeHelper.getTableMap().values());
        List<Table> returnList = DmpAnalyzeUtils.clone(valuesList);
        DmpAnalyzeHelper.removeTableMap();
        AnalyzeReturnType art = new AnalyzeReturnType();
        // 获取所有的create语句
        List<String> allCreateTableSqlList = returnList.stream().map(Table::getCreate_table).collect(Collectors.toList());
        // 获取所有的insertInto语句
        List<List<String>> allInsertIntoSqlList = returnList.stream().map(Table::getInsertSql).collect(Collectors.toList());
        List<String> aiisl = new ArrayList<>();
        for (List<String> allInsertIntoSql : allInsertIntoSqlList) {
            aiisl.addAll(allInsertIntoSql);
        }
        art.setAllCreateTableSqlList(allCreateTableSqlList);
        art.setAllInsertIntoSqlList(aiisl);
        log.debug("<<--------------------建表语句-------------------->>");
        for (String c : art.getAllCreateTableSqlList()) {
            log.debug(c);
        }
        log.debug("<<--------------------insert语句-------------------->>");
        for (String i : art.getAllInsertIntoSqlList()) {
            log.debug(i);
        }
        return art;
    }
}
