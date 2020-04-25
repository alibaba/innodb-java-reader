/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.alibaba.innodb.java.reader.CharsetMapping;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.Constants.DEFAULT_JAVA_CHARSET;
import static com.alibaba.innodb.java.reader.Constants.DEFAULT_MYSQL_CHARSET;
import static com.alibaba.innodb.java.reader.column.ColumnType.CHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Schema description, like <code>SHOW CREATE TABLE LIKE 'TTT'</code>.
 *
 * @author xu.zx
 */
@EqualsAndHashCode
@Slf4j
public class Schema {

  private List<String> columnNames;

  private List<Column> columnList;

  private Map<String, Field> nameToFieldMap;

  private Column primaryKeyColumn;

  private int nullableColumnNum = 0;

  private int variableLengthColumnNum = 0;

  private List<Column> nullableColumnList;

  private List<Column> variableLengthColumnList;

  private int ordinal = 0;

  /**
   * For decoding string in Java.
   */
  private String javaCharset = DEFAULT_JAVA_CHARSET;

  /**
   * Table DDL charset, for example can be latin, utf8, utf8mb4.
   */
  private String charset = DEFAULT_MYSQL_CHARSET;

  /**
   * //TODO make sure this is the right way to implement
   * For example, if table charset set to utf8, then it will consume up to 3 bytes for one character.
   * if it is utf8mb4, then it must be set to 4.
   */
  private int maxBytesPerChar = 1;

  public Schema() {
    this.columnList = new ArrayList<>();
    this.columnNames = new ArrayList<>();
    this.nameToFieldMap = new HashMap<>();
    this.nullableColumnList = new ArrayList<>();
    this.variableLengthColumnList = new ArrayList<>();
  }

  public void validate() {
    checkState(CollectionUtils.isNotEmpty(columnList), "No column is specified");
  }

  public boolean containsVariableLengthColumn() {
    return variableLengthColumnNum > 0;
  }

  public boolean containsNullColumn() {
    return nullableColumnNum > 0;
  }

  public List<Column> getColumnList() {
    return columnList;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public int getColumnNum() {
    return columnNames.size();
  }

  public int getNullableColumnNum() {
    return nullableColumnNum;
  }

  public int getVariableLengthColumnNum() {
    return variableLengthColumnNum;
  }

  public Schema addColumn(Column column) {
    checkNotNull(column, "Column should not be null");
    checkArgument(StringUtils.isNotEmpty(column.getName()), "Column name is empty");
    checkArgument(StringUtils.isNotEmpty(column.getType()), "Column type is empty");
    checkArgument(!nameToFieldMap.containsKey(column.getName()), "Duplicate column name");
    if (column.isPrimaryKey()) {
      checkState(primaryKeyColumn == null, "Primary key is already defined");
      primaryKeyColumn = column;
    }
    if (column.isNullable()) {
      nullableColumnList.add(column);
      nullableColumnNum++;
    }
    if (column.isVariableLength()) {
      variableLengthColumnList.add(column);
      variableLengthColumnNum++;
    } else if (CHAR.equals(column.getType()) && maxBytesPerChar > 1) {
      // 多字符集则设置为varchar的读取方式
      column.setVarLenChar(true);
      variableLengthColumnList.add(column);
      variableLengthColumnNum++;
    }
    column.setSchema(this);
    columnList.add(column);
    columnNames.add(column.getName());
    nameToFieldMap.put(column.getName(), new Field(ordinal++, column.getName(), column));
    return this;
  }

  public Field getField(String columnName) {
    return nameToFieldMap.get(columnName);
  }

  public Column getPrimaryKeyColumn() {
    return primaryKeyColumn;
  }

  public void setPrimaryKeyColumn(Column primaryKeyColumn) {
    this.primaryKeyColumn = primaryKeyColumn;
  }

  public List<Column> getVariableLengthColumnList() {
    return variableLengthColumnList;
  }

  public List<Column> getNullableColumnList() {
    return nullableColumnList;
  }

  public String getJavaCharset() {
    return javaCharset;
  }

  public String getCharset() {
    return charset;
  }

  public Schema setCharset(String charset) {
    checkArgument(CollectionUtils.isEmpty(columnList), "Charset should be set before adding columns");
    this.charset = charset;
    this.javaCharset =  CharsetMapping.getJavaCharsetForMysqlCharset(charset);
    this.maxBytesPerChar = CharsetMapping.getMaxByteLengthForMysqlCharset(charset);
    return this;
  }

  public int getMaxBytesPerChar() {
    return maxBytesPerChar;
  }

  @Data
  public class Field {
    private int ordinal;
    private String name;
    private Column column;

    public Field(int ordinal, String name, Column column) {
      this.ordinal = ordinal;
      this.name = name;
      this.column = column;
    }
  }

  @Override
  public String toString() {
    return toString(false);
  }

  public String toString(boolean multiLine) {
    StringBuilder sb = new StringBuilder();
    sb.append("Table columns");
    sb.append(" (tableCharset=");
    sb.append(charset);
    sb.append("):");
    for (Column column : columnList) {
      sb.append(multiLine ? "\n" : ",");
      sb.append(column.getName()).append(" ");
      sb.append(column.getType());
      //TODO add extra info like maxVarLen, time precision, and decimal precision and scale
      sb.append(" ");
      if (!column.isNullable()) {
        sb.append("NOT NULL ");
      }
      if (column.isPrimaryKey()) {
        sb.append("PRIMARY KEY");
      }
    }
    return sb.toString();
  }
}
