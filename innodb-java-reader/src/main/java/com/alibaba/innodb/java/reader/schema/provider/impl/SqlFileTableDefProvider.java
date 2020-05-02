/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema.provider.impl;

import com.google.common.collect.ImmutableMap;

import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.TableDefUtil;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;
import com.alibaba.innodb.java.reader.util.Pair;
import com.alibaba.innodb.java.reader.util.Utils;

import java.io.File;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Use <tt>CREATE TABLE</tt> SQL file as table definition provider.
 *
 * @author xu.zx
 */
public class SqlFileTableDefProvider implements TableDefProvider {

  public static final String CREATE_TABLE_LITERAL = "CREATE TABLE";

  public static final int CREATE_TABLE_LITERAL_LENGTH = CREATE_TABLE_LITERAL.length();

  private String createTableSqlFilePath;

  private String charset;

  public SqlFileTableDefProvider(String createTableSqlFilePath) {
    this(createTableSqlFilePath, "UTF-8");
  }

  public SqlFileTableDefProvider(String createTableSqlFilePath, String charset) {
    checkNotNull(createTableSqlFilePath);
    if (!new File(createTableSqlFilePath).exists()) {
      throw new IllegalArgumentException("createTableSqlFilePath " + createTableSqlFilePath + " not exist");
    }
    this.createTableSqlFilePath = createTableSqlFilePath;
    this.charset = charset;
  }

  @Override
  public Map<String, TableDef> load() {
    ImmutableMap.Builder<String, TableDef> builder = new ImmutableMap.Builder<>();
    Pair<Integer, String> result = Utils.processFileWithDelimiter(createTableSqlFilePath, charset, content -> {
      content = content.trim();
      if (content.length() > CREATE_TABLE_LITERAL_LENGTH) {
        if (content.substring(0, CREATE_TABLE_LITERAL_LENGTH).equalsIgnoreCase(CREATE_TABLE_LITERAL)) {
          TableDef tableDef = TableDefUtil.covertToTableDef(content);
          builder.put(tableDef.getFullyQualifiedName(), tableDef);
        }
      }
    }, ";");

    // no semi-colon found
    if (result.getFirst() == 0) {
      TableDef tableDef = TableDefUtil.covertToTableDef(result.getSecond());
      builder.put(tableDef.getFullyQualifiedName(), tableDef);
    }

    return builder.build();
  }

}
