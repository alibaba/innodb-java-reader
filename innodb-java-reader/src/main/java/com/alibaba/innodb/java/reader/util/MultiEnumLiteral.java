/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * SET column type result type.
 *
 * @author xu.zx
 */
@EqualsAndHashCode
public class MultiEnumLiteral implements Comparable<MultiEnumLiteral> {

  private final List<Integer> maskList;

  private final List<String> valueList;

  public MultiEnumLiteral(int size) {
    this.maskList = new ArrayList<>(size);
    this.valueList = new ArrayList<>(size);
  }

  public MultiEnumLiteral add(int mask, String value) {
    maskList.add(mask);
    valueList.add(value);
    return this;
  }

  public List<Integer> getMaskList() {
    return maskList;
  }

  public List<String> getValueList() {
    return valueList;
  }

  @Override
  public int compareTo(MultiEnumLiteral o) {
    checkNotNull(o);
    checkNotNull(o.valueList);
    Collections.sort(this.valueList);
    Collections.sort(o.valueList);
    for (int i = 0; i < Math.min(this.valueList.size(), o.valueList.size()); i++) {
      int compare = this.valueList.get(i).compareToIgnoreCase(o.valueList.get(i));
      if (compare != 0) {
        return compare;
      }
    }
    return Integer.compare(this.valueList.size(), o.valueList.size());
  }

  @Override
  public String toString() {
    if (valueList == null) {
      return "";
    }
    return valueList.stream().collect(Collectors.joining(","));
  }

}
