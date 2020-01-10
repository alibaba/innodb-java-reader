/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.alibaba.innodb.java.reader.Constants;
import com.alibaba.innodb.java.reader.column.ColumnType;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.ToString;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Column description
 *
 * @author xu.zx
 */
@Data
public class Column {

  private String name;

  private String type;

  private boolean nullable;

  private boolean isPrimaryKey;

  /**
   * Note this can only be set internally.
   */
  private int maxVarLen;

  /**
   * //FIXME
   * for charset like utf8mb4, char will be treated as varchar.
   * Note this can only be set internally.
   */
  @ToString.Exclude
  private boolean isVarLenChar;

  public Column setType(final String type) {
    String t = type.trim();
    checkNotNull(t);
    if (!t.contains(Constants.Symbol.SPACE)) {
      extractTypeAndMaxLen(t);
    } else {
      String[] part = t.split(Constants.Symbol.SPACE);
      extractTypeAndMaxLen(part[0]);
      extractSigned(part[1]);
    }
    return this;
  }

  public Column setName(String name) {
    this.name = name.replace(Constants.Symbol.BACKTICK, Constants.Symbol.EMPTY);
    return this;
  }

  public Column setNullable(boolean nullable) {
    this.nullable = nullable;
    return this;
  }

  public Column setPrimaryKey(boolean primaryKey) {
    isPrimaryKey = primaryKey;
    return this;
  }

  public boolean isVariableLength() {
    if (isVarLenChar) {
      return true;
    }
    return ColumnType.VARIABLE_LENGTH_TYPES.contains(type);
  }

  public boolean isFixedLength() {
    return ColumnType.CHAR.equals(type) || ColumnType.BINARY.equals(type);
  }

  private void extractTypeAndMaxLen(String type) {
    if (type.contains(Constants.Symbol.LEFT_PARENTHESES) && type.contains(Constants.Symbol.RIGHT_PARENTHESES)) {
      this.type = type.toUpperCase().substring(0, type.indexOf("("));
      this.maxVarLen = Integer.parseInt(StringUtils.substringBetween(type, "(", ")"));
    } else {
      this.type = type.toUpperCase();
    }
  }

  private void extractSigned(String sign) {
    if (Constants.CONST_UNSIGNED_LOWER.contains(sign) || Constants.CONST_UNSIGNED_UPPER.contains(sign)) {
      this.type = type.toUpperCase() + " UNSIGNED";
    }
  }

}