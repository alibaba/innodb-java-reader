/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.google.common.base.Joiner;

import com.alibaba.innodb.java.reader.exception.SqlParseException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * SchemaUtil
 *
 * @author xu.zx
 */
@Slf4j
public class SchemaUtil {

  public static Schema covertFromSqlToSchema(String sql) {
    CreateTable stmt;
    try {
      log.debug("sql is {}", sql);
      stmt = (CreateTable) CCJSqlParserUtil.parse(sql);
    } catch (JSQLParserException e) {
      throw new SqlParseException("parse create table sql " + sql + " failed " + e.getMessage(), e);
    }
    checkNotNull(stmt, "CreateTable statement can not be null");
    Schema schema = new Schema();
    handleCharset(stmt, schema);
    handleColumns(stmt, schema);
    handleIndex(stmt, schema);

    log.debug("origin sql:" + sql);
    log.debug("parsed sql:" + stmt);
    return schema;
  }

  private static void handleCharset(CreateTable stmt, Schema schema) {
    List<?> tableOptions = stmt.getTableOptionsStrings();
    int indexOfCharset = tableOptions.indexOf("CHARSET");
    if (indexOfCharset > 0) {
      schema.setTableCharset((String) tableOptions.get(indexOfCharset + 2));
    }
  }

  private static void handleIndex(CreateTable stmt, Schema schema) {
    List<Index> indices = stmt.getIndexes();
    if (CollectionUtils.isEmpty(indices)) {
      throw new SqlParseException("no indices found, there is at least on primary key index");
    }
    for (Index index : indices) {
      if ("PRIMARY KEY".equals(index.getType().toUpperCase())) {
        List<String> colNames = index.getColumnsNames();
        if (CollectionUtils.isEmpty(colNames) || colNames.size() > 1) {
          throw new SqlParseException("only one column supported for primary key");
        }
        String pkColumnName = colNames.get(0).replace("`", "");
        if (schema.getColumnNames().contains(pkColumnName)) {
          Column pk = schema.getField(pkColumnName).getColumn();
          pk.setPrimaryKey(true);
          schema.setPrimaryKeyColumn(pk);
        }
      }
    }
  }

  private static void handleColumns(CreateTable stmt, Schema schema) {
    // parse columns
    if (CollectionUtils.isEmpty(stmt.getColumnDefinitions())) {
      throw new SqlParseException("cannot found any column");
    }
    for (ColumnDefinition col : stmt.getColumnDefinitions()) {
      Column column = new Column();
      column.setName(col.getColumnName());
      column.setType(col.getColDataType().getDataType());
      List<String> argList = col.getColDataType().getArgumentsStringList();
      if (CollectionUtils.isNotEmpty(argList)) {
        if (argList.size() > 1) {
          throw new SqlParseException("column " + col.getColumnName() + " contains more than one argument");
        }
        column.setType(column.getType() + "(" + argList.get(0) + ")");
      }
      if (col.getColumnSpecStrings() != null) {
        List<String> specList = col.getColumnSpecStrings().stream().map(String::toUpperCase).collect(Collectors.toList());
        if (specList.contains("UNSIGNED")) {
          column.setType(column.getType() + " UNSIGNED");
        }
        if (Joiner.on(" ").join(specList).contains("NOT NULL")) {
          column.setNullable(false);
        } else {
          column.setNullable(true);
        }
      }
      schema.addColumn(column);
    }
  }

}
