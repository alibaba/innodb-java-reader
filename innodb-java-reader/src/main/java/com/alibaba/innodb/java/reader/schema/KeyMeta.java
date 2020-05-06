/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;

/**
 * Key metadata information including name, number of columns and page number, etc.
 * <pre>
 * select * from INNODB_SYS_INDEXES;
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
   * Key (index) name if present or else null.
   */
  private String name;

  private Type type;

  /**
   * N_FIELDS.
   */
  private int numOfColumns;

  private List<Column> keyColumns;

  private List<String> keyColumnNames;

  private List<Column> keyVarLenColumns;

  private List<String> keyVarLenColumnNames;

  public boolean containsColumn(String columnName) {
    return keyColumnNames.contains(columnName);
  }

  public enum Type {
    /**
     * Key type.
     */
    KEY("KEY"),
    UNIQUE_KEY("UNIQUE KEY"),
    INDEX("INDEX"),
    UNIQUE_INDEX("UNIQUE INDEX"),
    PRIMARY_KEY("PRIMARY KEY");

    private String literal;

    Type(String literal) {
      this.literal = literal;
    }

    public String literal() {
      return literal;
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
