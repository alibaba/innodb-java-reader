/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.fsphdr;

import com.alibaba.innodb.java.reader.util.IdAble;

/**
 * Extent state.
 *
 * @author xu.zx
 */
public enum XdesState implements IdAble<Integer> {

  /** free extent */
  FREE(1,
      "The extent is completely empty and unused, and should be "
      + "present on the filespace's FREE list"),
  /** free fragment extent */
  FREE_FRAG(2,
      "Some pages of the extent are used individually, "
      + "and the extent should be present on the filespace's FREE_FRAG list"),
  /** full extent */
  FULL_FRAG(3,
      "All pages of the extent are used individually, and "
      + "the extent should be present on the filespace's FULL_FRAG list"),
  /** extent that belongs to one segment */
  FSEG(4,
      "The extent is wholly allocated to a file segment."
      + " Additional information about the state of this extent"
      + " can be derived from the its presence on particular"
      + " file segment lists (FULL, NOT_FULL, or FREE)");

  private int type;

  private String value;

  XdesState(int type, String value) {
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

}
