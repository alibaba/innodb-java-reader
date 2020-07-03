/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import lombok.EqualsAndHashCode;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ENUM column type result type.
 *
 * @author xu.zx
 */
@EqualsAndHashCode
public class SingleEnumLiteral implements Comparable<SingleEnumLiteral> {

  private final int ordinal;

  private final String value;

  public SingleEnumLiteral(int ordinal, String value) {
    this.ordinal = ordinal;
    this.value = value;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public String getValue() {
    return value;
  }

  @Override
  public int compareTo(SingleEnumLiteral o) {
    checkNotNull(o);
    checkNotNull(o.value);
    return value.compareToIgnoreCase(o.value);
  }

  @Override
  public String toString() {
    return value;
  }

}
