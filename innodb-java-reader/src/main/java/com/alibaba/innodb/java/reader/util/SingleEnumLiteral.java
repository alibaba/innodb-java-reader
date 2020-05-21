/**
 * Apache License Version 2.0.
 *
 * Copy from https://github.com/rolandhe/hiriver
 */
package com.alibaba.innodb.java.reader.util;

/**
 * ENUM column type result type.
 *
 * @author xu.zx
 */
public class SingleEnumLiteral {

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
  public String toString() {
    return value;
  }
}
