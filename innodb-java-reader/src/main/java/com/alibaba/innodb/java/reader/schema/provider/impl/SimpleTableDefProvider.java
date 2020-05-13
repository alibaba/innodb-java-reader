/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema.provider.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.alibaba.innodb.java.reader.util.Utils.noneEmpty;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple table definition provider.
 *
 * @author xu.zx
 */
public class SimpleTableDefProvider implements TableDefProvider {

  private List<TableDef> tableDefList;

  public SimpleTableDefProvider(List<TableDef> tableDefList) {
    checkNotNull(tableDefList, "tableDefList cannot be null");
    checkArgument(noneEmpty(tableDefList), "tableDefList contains null");
    this.tableDefList = tableDefList;
  }

  public SimpleTableDefProvider(TableDef tableDef) {
    this(ImmutableList.of(tableDef));
  }

  @Override
  public Map<String, TableDef> load() {
    ImmutableMap.Builder<String, TableDef> builder = new ImmutableMap.Builder<>();
    if (CollectionUtils.isNotEmpty(tableDefList)) {
      for (TableDef tableDef : tableDefList) {
        if (StringUtils.isEmpty(tableDef.getName())) {
          throw new IllegalArgumentException("TableDef should have full qualified name (table name)");
        }
        builder.put(tableDef.getName(), tableDef);
      }
    }
    return builder.build();
  }

}
