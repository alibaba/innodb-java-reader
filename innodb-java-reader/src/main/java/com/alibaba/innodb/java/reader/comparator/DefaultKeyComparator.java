/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.comparator;

import com.alibaba.innodb.java.reader.schema.Column;

import java.util.List;

import static com.alibaba.innodb.java.reader.util.Utils.castCompare;

/**
 * Default key comparator.
 *
 * @author xu.zx
 */
public class DefaultKeyComparator implements KeyComparator {

  @Override
  public int compare(List<Object> recordKey, List<Object> targetKey, List<Column> keyColumnList) {
    return castCompare(recordKey, targetKey, keyColumnList);
  }

}
