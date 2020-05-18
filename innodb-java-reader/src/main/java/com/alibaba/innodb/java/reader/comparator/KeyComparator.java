/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.comparator;

import com.alibaba.innodb.java.reader.schema.Column;

import java.util.List;

/**
 * Key comparator.
 *
 * @author xu.zx
 */
public interface KeyComparator {

  /**
   * Compare record key and target key.
   *
   * @param recordKey     record key
   * @param targetKey     target key
   * @param keyColumnList key column list
   * @return comparing result
   */
  int compare(List<Object> recordKey, List<Object> targetKey, List<Column> keyColumnList);

}
