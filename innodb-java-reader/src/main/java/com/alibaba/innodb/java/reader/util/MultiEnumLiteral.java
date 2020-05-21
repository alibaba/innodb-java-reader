/**
 * Apache License Version 2.0.
 *
 * Copy from https://github.com/rolandhe/hiriver
 */
package com.alibaba.innodb.java.reader.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SET column type result type.
 *
 * @author xu.zx
 */
public class MultiEnumLiteral {

  private final List<Integer> maskList;

  private final List<String> valueList;

  public MultiEnumLiteral(int size) {
    this.maskList = new ArrayList<>(size);
    this.valueList = new ArrayList<>(size);
  }

  public void add(int mask, String value) {
    maskList.add(mask);
    valueList.add(value);
  }

  public List<Integer> getMaskList() {
    return maskList;
  }

  public List<String> getValueList() {
    return valueList;
  }

  @Override
  public String toString() {
    if (valueList == null) {
      return "";
    }
    return valueList.stream().collect(Collectors.joining(","));
  }
}
