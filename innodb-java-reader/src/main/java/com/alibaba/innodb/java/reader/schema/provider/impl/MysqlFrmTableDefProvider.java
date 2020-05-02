/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema.provider.impl;

import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;

import java.util.Map;

/**
 * //TODO
 *
 * @author xu.zx
 */
public class MysqlFrmTableDefProvider implements TableDefProvider {

  @Override
  public Map<String, TableDef> load() {
    throw new UnsupportedOperationException();
  }

}
