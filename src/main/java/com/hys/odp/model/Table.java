package com.hys.odp.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Data
@NoArgsConstructor
public class Table implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * 表声明
     */
    private String statement;
    /**
     * 创建表语句
     */
    private String create_table;
    /**
     * 插入批处理语句
     */
    private String insert_into;
    /**
     * 数据行
     */
    private final List<Row> rows = new ArrayList<>();
    /**
     * 存储数据类型 字符串数组
     */
    private String[] dataType;
    /**
     * 存储数据类型 集合
     */
    private ArrayList<String> dataTypeList = new ArrayList<>();
    /**
     * 拼接后的insert语句
     */
    private List<String> insertSql = new ArrayList<>();

    public Table(String statement, String create_table, String insert_into) {
        this.statement = statement;
        this.create_table = create_table;
        this.insert_into = insert_into;
        readddd(create_table);
    }

    //根据create语句 查出 字段个数 字段类型(顺序)
    //1根据左括号的位置 查出对应右括号的位置
    //根据substring切割字符串即可
    private void readddd(String create_table) {
        int left = 0;
        int right = 0;
        int end = 0;
        for (int i = 0; i < create_table.length(); i++) {
            if (create_table.charAt(i) == '(') {
                if (left == 0) {
                    right = i;
                }
                left++;
            }
            if (create_table.charAt(i) == ')') {
                left--;
                if (left == 0) {
                    end = i;
                    break;
                }
            }
        }
        //create语句中的语句最外层括号的值;
        String tempSubString = create_table.substring(right + 1, end);
        String[] tempStringArray = tempSubString.split("\" ");
        for (int i = 1; i < tempStringArray.length; i++) {
            String[] strArray = tempStringArray[i].split(",");
            dataTypeList.add(strArray[0]);
        }
    }
}
