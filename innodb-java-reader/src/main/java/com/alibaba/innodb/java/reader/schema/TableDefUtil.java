/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.exception.SqlParseException;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Locale;
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
      sql = reviseSql(sql);
      stmt = (CreateTable) CCJSqlParserUtil.parse(sql);
      checkNotNull(stmt, "CreateTable statement should not be null");
      TableDef tableDef = new TableDef();
      tableDef.setName(stmt.getTable().getName());
      tableDef.setFullyQualifiedName(stmt.getTable().getFullyQualifiedName());
      // set charset first
      handleCharset(stmt, tableDef);
      handleCollation(stmt, tableDef);
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
      int indexOfCharacterSet = tableOptions.indexOf("CHARACTER");
      if (indexOfCharacterSet == -1) {
        indexOfCharacterSet = tableOptions.indexOf("CHARACTER");
      }
      if (indexOfCharacterSet > 0) {
        tableDef.setDefaultCharset((String) tableOptions.get(indexOfCharacterSet + 2));
      }
    }
  }

  private static void handleCollation(CreateTable stmt, TableDef tableDef) {
    List<?> tableOptions = stmt.getTableOptionsStrings();
    if (CollectionUtils.isNotEmpty(tableOptions)) {
      int indexOfCollate = tableOptions.indexOf("COLLATE");
      if (indexOfCollate == -1) {
        indexOfCollate = tableOptions.indexOf("collate");
      }
      if (indexOfCollate > 0) {
        String collateNextOne = (String) tableOptions.get(indexOfCollate + 1);
        if (!"=".equals(collateNextOne)) {
          tableDef.setCollation(collateNextOne);
        } else {
          tableDef.setCollation((String) tableOptions.get(indexOfCollate + 2));
        }
      }
    }
  }

  private static void handleIndex(CreateTable stmt, TableDef tableDef) {
    List<Index> indices = stmt.getIndexes();
    if (CollectionUtils.isNotEmpty(indices)) {
      for (Index index : indices) {
        if (KeyMeta.Type.isValid(index.getType())) {
          List<String> colNames = getColumnNames(index);
          if (KeyMeta.Type.PRIMARY_KEY.literal().equalsIgnoreCase(index.getType())) {
            // handle pk
            tableDef.setPrimaryKeyColumns(colNames);
          } else if (KeyMeta.Type.isValidSk(index.getType())) {
            // handle sk
            tableDef.addSecondaryKeyColumns(index.getType().toUpperCase(),
                index.getName(), colNames);
          } else {
            // handle fulltext and foreign key
            tableDef.addSecondaryKeyColumns(index.getType().toUpperCase(),
                index.getName(), ImmutableList.of());
          }
        } else {
          throw new SqlParseException("Index type is invalid " + index.getType());
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
          column.setType(column.getFullType() + " UNSIGNED");
        }
        String specString = Joiner.on(" ").join(specList);
        if (specString.contains("NOT NULL")) {
          column.setNullable(false);
        }
        if (specString.contains("PRIMARY KEY") || specString.contains("KEY")) {
          column.setPrimaryKey(true);
        }
        if (specString.contains("CHARSET")) {
          column.setCharset(specList.get(specList.indexOf("CHARSET") + 1).toLowerCase(Locale.ROOT));
        }
        if (specString.contains("COLLATE")) {
          column.setCollation(specList.get(specList.indexOf("COLLATE") + 1).toLowerCase(Locale.ROOT));
        }
      }
      tableDef.addColumn(column);
    }
  }

  private static List<String> getColumnNames(Index index) {
    List<String> colNames = index.getColumnsNames();
    if (CollectionUtils.isEmpty(colNames)) {
      throw new SqlParseException("No column specified for index " + index);
    }
    return colNames;
  }

  /**
   * Workaround for cases when jsqlparser cannot work properly.
   * <p>
   * For example, the following sql will fail due to
   * <code>Caused by: net.sf.jsqlparser.parser.ParseException:
   * Encountered unexpected token: "KEY" "KEY"</code>, so it will be handel properly.
   * <pre>
   * CREATE TABLE `test2` (
   *   `key` int(11) NOT NULL,
   *   `value` varchar(96) DEFAULT NULL,
   *    PRIMARY KEY `PRIMARY` (`key`)
   * ) ENGINE=InnoDB;
   * </pre>
   *
   * @param sql origin sql
   * @return revised sql
   */
  private static String reviseSql(String sql) {
    String sqlUpperCase = sql.toUpperCase(Locale.ROOT);
    int indexOfPkPk = sqlUpperCase.indexOf("PRIMARY KEY `PRIMARY`");
    if (indexOfPkPk > 0) {
      sql = sql.substring(0, indexOfPkPk + 11) + sql.substring(indexOfPkPk + 21);
    }
    return sql;
  }

}
