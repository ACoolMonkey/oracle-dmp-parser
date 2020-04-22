package com.hys.odp.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 解析出的结果（创建表和插入表sql语句的集合）
 *
 * @author Robert Hou
 * @date 2019年3月21日
 */
@Data
@NoArgsConstructor
public class AnalyzeReturnType implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * 本次解析所有的创建表语句
     */
    private List<String> allCreateTableSqlList;
    /**
     * 本次解析所有的插入表语句
     */
    private List<String> allInsertIntoSqlList;
}
