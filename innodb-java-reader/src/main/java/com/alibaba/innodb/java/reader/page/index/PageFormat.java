/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.util.IdAble;

import java.util.HashMap;
import java.util.Map;

/**
 * PageFormat
 *
 * @author xu.zx
 */
public enum PageFormat implements IdAble<Integer> {

  /** redundant format */
  REDUNDANT(0, "redundant"),
  /** compact format */
  COMPACT(1, "compact");

  private int type;

  private String value;

  PageFormat(int type, String value) {
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

  private static Map<Integer, PageFormat> KVS = new HashMap<>(values().length);

  static {
    for (PageFormat recordType : values()) {
      KVS.put(recordType.type(), recordType);
    }
  }

  public static PageFormat parse(int type) {
    return KVS.get(type);
  }
}
