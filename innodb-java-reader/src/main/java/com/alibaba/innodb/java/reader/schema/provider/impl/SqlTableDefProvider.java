/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema.provider.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.TableDefUtil;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.alibaba.innodb.java.reader.util.Utils.noneEmpty;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Use <tt>CREATE TABLE</tt> SQL string as table definition provider.
 *
 * @author xu.zx
 */
public class SqlTableDefProvider implements TableDefProvider {

  private List<String> createTableSqlList;

  public SqlTableDefProvider(List<String> createTableSqlList) {
    checkNotNull(createTableSqlList, "createTableSqlList cannot be null");
    checkArgument(noneEmpty(createTableSqlList), "createTableSqlList contains null");
    this.createTableSqlList = createTableSqlList;
  }

  public SqlTableDefProvider(String createTableSql) {
    this(ImmutableList.of(createTableSql));
  }

  @Override
  public Map<String, TableDef> load() {
    ImmutableMap.Builder<String, TableDef> builder = new ImmutableMap.Builder<>();
    if (CollectionUtils.isNotEmpty(createTableSqlList)) {
      for (String createTableSql : createTableSqlList) {
        if (StringUtils.isEmpty(createTableSql)) {
          continue;
        }
        TableDef tableDef = TableDefUtil.covertToTableDef(createTableSql);
        builder.put(tableDef.getFullyQualifiedName(), tableDef);
      }
    }
    return builder.build();
  }

}
