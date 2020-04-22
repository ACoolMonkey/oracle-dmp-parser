package com.hys.odp.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class Row implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * 列
     */
    private final List<Column> cols = new ArrayList<>();
}
