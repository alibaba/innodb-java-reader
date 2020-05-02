/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema.provider;

import com.alibaba.innodb.java.reader.schema.TableDef;

import java.util.Map;

/**
 * Table definition provider.
 *
 * @author xu.zx
 */
public interface TableDefProvider {

  /**
   * Load full qualified name to TableDef map.
   *
   * @return map
   */
  Map<String, TableDef> load();

}
