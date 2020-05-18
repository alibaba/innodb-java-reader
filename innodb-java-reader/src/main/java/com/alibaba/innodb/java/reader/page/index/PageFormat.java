/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.util.IdAble;

/**
 * Page format.
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

  public static PageFormat parse(int type) {
    if (type == REDUNDANT.type) {
      return REDUNDANT;
    } else if (type == COMPACT.type) {
      return COMPACT;
    }
    return null;
  }
}
