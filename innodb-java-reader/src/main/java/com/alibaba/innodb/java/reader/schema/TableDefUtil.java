/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.google.common.base.Joiner;

import com.alibaba.innodb.java.reader.exception.SqlParseException;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TableDef utility.
 *
 * @author xu.zx
 */
@Slf4j
public class TableDefUtil {

  public static TableDef covertToTableDef(String sql) {
    CreateTable stmt;
    try {
      stmt = (CreateTable) CCJSqlParserUtil.parse(sql);
      checkNotNull(stmt, "CreateTable statement should not be null");
      TableDef tableDef = new TableDef();
      tableDef.setName(stmt.getTable().getName());
      tableDef.setFullyQualifiedName(stmt.getTable().getFullyQualifiedName());
      // set charset first
      handleCharset(stmt, tableDef);
      handleColumns(stmt, tableDef);
      handleIndex(stmt, tableDef);

      if (log.isDebugEnabled()) {
        log.debug("origin sql:" + sql);
        log.debug("parsed sql:" + stmt);
      }
      return tableDef;
    } catch (Exception e) {
      throw new SqlParseException("Parse create table sql failed " + sql, e);
    }
  }

  private static void handleCharset(CreateTable stmt, TableDef tableDef) {
    List<?> tableOptions = stmt.getTableOptionsStrings();
    if (CollectionUtils.isNotEmpty(tableOptions)) {
      int indexOfCharset = tableOptions.indexOf("CHARSET");
      if (indexOfCharset == -1) {
        indexOfCharset = tableOptions.indexOf("charset");
      }
      if (indexOfCharset > 0) {
        tableDef.setDefaultCharset((String) tableOptions.get(indexOfCharset + 2));
      }
    }
  }

  private static void handleIndex(CreateTable stmt, TableDef tableDef) {
    List<Index> indices = stmt.getIndexes();
    if (CollectionUtils.isNotEmpty(indices)) {
      for (Index index : indices) {
        if ("PRIMARY KEY".equalsIgnoreCase(index.getType())) {
          List<String> colNames = getColumnNames(index);
          tableDef.setPrimaryKeyColumns(colNames);
        }

        if ("KEY".equalsIgnoreCase(index.getType())
            || "INDEX".equalsIgnoreCase(index.getType())
            || "UNIQUE KEY".equalsIgnoreCase(index.getType())
            || "UNIQUE INDEX".equalsIgnoreCase(index.getType())) {
          List<String> colNames = getColumnNames(index);
          tableDef.addSecondaryKeyColumns(index.getType().toUpperCase(), index.getName(), colNames);
        }
      }
    }
  }

  private static void handleColumns(CreateTable stmt, TableDef tableDef) {
    // parse columns
    if (CollectionUtils.isEmpty(stmt.getColumnDefinitions())) {
      throw new SqlParseException("Cannot found any column");
    }
    for (ColumnDefinition col : stmt.getColumnDefinitions()) {
      Column column = new Column();
      column.setName(col.getColumnName());
      List<String> argList = col.getColDataType().getArgumentsStringList();
      if (CollectionUtils.isNotEmpty(argList)) {
        if (argList.size() > 2) {
          throw new SqlParseException("Column " + col.getColumnName()
              + " contains more than two argument, " + argList);
        }
        column.setType(col.getColDataType().getDataType()
            + "(" + argList.stream().collect(Collectors.joining(",")) + ")");
      } else {
        column.setType(col.getColDataType().getDataType());
      }
      if (StringUtils.isNotEmpty(col.getColDataType().getCharacterSet())) {
        column.setCharset(col.getColDataType().getCharacterSet());
      }
      column.setNullable(true);
      if (col.getColumnSpecStrings() != null) {
        List<String> specList = col.getColumnSpecStrings().stream()
            .map(String::toUpperCase).collect(Collectors.toList());
        if (specList.contains("UNSIGNED")) {
          column.setType(column.getType() + " UNSIGNED");
        }
        String specString = Joiner.on(" ").join(specList);
        if (specString.contains("NOT NULL")) {
          column.setNullable(false);
        }
        if (specString.contains("PRIMARY KEY") || specString.contains("KEY")) {
          column.setPrimaryKey(true);
        }
      }
      tableDef.addColumn(column);
    }
  }

  private static List<String> getColumnNames(Index index) {
    List<String> colNames = index.getColumnsNames();
    if (CollectionUtils.isEmpty(colNames)) {
      throw new SqlParseException("No column specified by PRIMARY KEY");
    }
    return colNames;
  }

}
