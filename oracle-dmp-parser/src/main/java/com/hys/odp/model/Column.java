package com.hys.odp.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class Column implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * 值
     */
    private byte[] value;
    /**
     * 各种数据类型
     */
    private Object object;

    public Column(Object object) {
        this.object = object;
    }

    public Column(byte[] value) {
        this.value = value;
    }
}
