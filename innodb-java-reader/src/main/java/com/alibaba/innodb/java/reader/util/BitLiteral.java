/**
 * Apache License Version 2.0.
 *
 * Copy from https://github.com/rolandhe/hiriver
 */
package com.alibaba.innodb.java.reader.util;

import lombok.EqualsAndHashCode;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_LONG;

/**
 * BIT column type result type.
 *
 * @author xu.zx
 */
@EqualsAndHashCode
public class BitLiteral implements Comparable<BitLiteral> {

  private final int len;

  private final byte[] bytes;

  public BitLiteral(byte[] bytes, int len) {
    this.bytes = bytes;
    this.len = len;
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public int compareTo(BitLiteral o) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (int i = bytes.length - 1; i >= 0; i--) {
      int start = (bytes.length - 1 - i) * SIZE_OF_LONG;
      for (int j = 0; j < SIZE_OF_LONG && start + j < len; j++) {
        if (((bytes[i] >> j) & 1) == 1) {
          result.append('1');
        } else {
          result.append('0');
        }
      }
    }
    return result.reverse().toString();
  }
}
