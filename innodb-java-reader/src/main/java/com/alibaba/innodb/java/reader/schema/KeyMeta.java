/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Key metadata information including name, number of columns and page number, etc.
 * <pre>
 * MySQL 5.7:  SELECT * FROM INFORMATION_SCHEMA.INNODB_SYS_INDEXES;
 * MySQL 8.0+: SELECT * FROM INFORMATION_SCHEMA.INNODB_INDEXES;
 * +----------+----------+----------+------+----------+---------+-------+
 * | INDEX_ID | NAME     | TABLE_ID | TYPE | N_FIELDS | PAGE_NO | SPACE |
 * +----------+----------+----------+------+----------+---------+-------+
 * |       11 | ID_IND   |       11 |    3 |        1 |     302 |     0 |
 * |       12 | FOR_IND  |       11 |    0 |        1 |     303 |     0 |
 * ...
 * </pre>
 *
 * @author xu.zx
 */
@Data
@Builder
public class KeyMeta {

  /**
   * Key (index) name if present.
   * <p>
   * Every index should have a name, if not specified when creating table,
   * MySQL will assign a name to it.
   */
  private String name;

  /**
   * Key type.
   */
  private Type type;

  /**
   * Number of columns, should be greater than 0.
   */
  private int numOfColumns;

  private List<Column> keyColumns;

  private List<String> keyColumnNames;

  private List<Column> keyVarLenColumns;

  private List<String> keyVarLenColumnNames;

  public boolean containsColumn(String columnName) {
    return keyColumnNames.contains(columnName);
  }

  public KeyMeta validate() {
    checkArgument(StringUtils.isNotEmpty(name), "Key name should not be empty " + this);
    if (Type.isValidSk(type.literal)) {
      checkArgument(numOfColumns > 0, "Number of columns should be greater than 0");
    }
    return this;
  }

  public enum Type {
    /**
     * Key type.
     */
    PRIMARY_KEY("PRIMARY KEY"),
    KEY("KEY"),
    INDEX("INDEX"),
    UNIQUE_KEY("UNIQUE KEY"),
    UNIQUE_INDEX("UNIQUE INDEX"),
    FULLTEXT_KEY("FULLTEXT KEY"),
    FOREIGN_KEY("FOREIGN KEY");

    private String literal;

    Type(String literal) {
      this.literal = literal;
    }

    public String literal() {
      return literal;
    }

    public static boolean isValid(String type) {
      return PRIMARY_KEY.literal.equalsIgnoreCase(type)
          || KEY.literal.equalsIgnoreCase(type)
          || INDEX.literal.equalsIgnoreCase(type)
          || UNIQUE_KEY.literal.equalsIgnoreCase(type)
          || UNIQUE_INDEX.literal.equalsIgnoreCase(type)
          || FULLTEXT_KEY.literal.equalsIgnoreCase(type)
          || FOREIGN_KEY.literal.equalsIgnoreCase(type);
    }

    public static boolean isValidSk(String type) {
      return KEY.literal.equalsIgnoreCase(type)
          || INDEX.literal.equalsIgnoreCase(type)
          || UNIQUE_KEY.literal.equalsIgnoreCase(type)
          || UNIQUE_INDEX.literal.equalsIgnoreCase(type);
    }

    // ---------- template method ---------- //

    private static Map<String, Type> KVS = Maps.newHashMapWithExpectedSize(values().length);

    static {
      for (Type type : values()) {
        KVS.put(type.literal, type);
      }
    }

    public static Type parse(String literal) {
      return KVS.get(literal);
    }
  }

}
