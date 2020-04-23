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
 * Schema utility.
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
      checkNotNull(stmt, "CreateTable statement can not be null");
      Schema schema = new Schema();
      handleCharset(stmt, schema);
      handleColumns(stmt, schema);
      handleIndex(stmt, schema);

      log.debug("origin sql:" + sql);
      log.debug("parsed sql:" + stmt);
      return schema;
    } catch (Exception e) {
      throw new SqlParseException("parse create table sql " + sql + " failed " + e.getMessage(), e);
    }
  }

  private static void handleCharset(CreateTable stmt, Schema schema) {
    List<?> tableOptions = stmt.getTableOptionsStrings();
    if (CollectionUtils.isNotEmpty(tableOptions)) {
      int indexOfCharset = tableOptions.indexOf("CHARSET");
      if (indexOfCharset > 0) {
        schema.setTableCharset((String) tableOptions.get(indexOfCharset + 2));
      }
    }
  }

  private static void handleIndex(CreateTable stmt, Schema schema) {
    List<Index> indices = stmt.getIndexes();
    if (CollectionUtils.isNotEmpty(indices)) {
      for (Index index : indices) {
        if ("PRIMARY KEY".equals(index.getType().toUpperCase())) {
          List<String> colNames = index.getColumnsNames();
          if (CollectionUtils.isEmpty(colNames) || colNames.size() > 1) {
            throw new SqlParseException("only one column supported for primary key currently");
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
  }

  private static void handleColumns(CreateTable stmt, Schema schema) {
    // parse columns
    if (CollectionUtils.isEmpty(stmt.getColumnDefinitions())) {
      throw new SqlParseException("cannot found any column");
    }
    for (ColumnDefinition col : stmt.getColumnDefinitions()) {
      Column column = new Column();
      column.setName(col.getColumnName());
      List<String> argList = col.getColDataType().getArgumentsStringList();
      if (CollectionUtils.isNotEmpty(argList)) {
        if (argList.size() > 2) {
          throw new SqlParseException("column " + col.getColumnName()
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
        if (specString.contains("PRIMARY KEY")) {
          column.setPrimaryKey(true);
        }
      }
      schema.addColumn(column);
    }
  }

}
