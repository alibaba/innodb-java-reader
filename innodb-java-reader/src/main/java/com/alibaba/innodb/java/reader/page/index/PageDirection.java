/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.util.IdAble;

import java.util.HashMap;
import java.util.Map;

/**
 * PageDirection
 *
 * @author xu.zx
 */
public enum PageDirection implements IdAble<Integer> {

  /** left */
  LEFT(1, "Inserts have been in descending order"),
  /** right */
  RIGHT(2, "Inserts have been in ascending order"),
  /** same record */
  SAME_REC(3, "Unused by InnoDB"),
  /** same page */
  SAME_PAGE(4, "Unused by InnoDB"),
  /** no direction */
  NO_DIRECTION(5, "Inserts have been in random order");

  private int type;

  private String value;

  PageDirection(int type, String value) {
    this.type = type;
    this.value = value;
  }

  public int type() {
    return type;
  }

  public String value() {
    return value;
  }

  @Override
  public Integer id() {
    return type;
  }

  // ---------- template method ---------- //

  private static Map<Integer, PageDirection> KVS = new HashMap<>(values().length);

  static {
    for (PageDirection recordType : values()) {
      KVS.put(recordType.type(), recordType);
    }
  }

  public static PageDirection parse(int type) {
    return KVS.get(type);
  }
}
