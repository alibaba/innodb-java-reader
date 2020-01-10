/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.alibaba.innodb.java.reader.column.ColumnType.DOUBLE;
import static com.alibaba.innodb.java.reader.column.ColumnType.FLOAT;
import static com.alibaba.innodb.java.reader.column.ColumnType.JAVA_INTEGER_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.JAVA_LONG_TYPES;

/**
 * Utils
 *
 * @author xu.zx
 */
public class Utils {

  public static final String MAX = "SUPREMUM";

  public static final String MIN = "INFIMUM";

  public static Object tryCastString(final Object object, String type) {
    if (!(object instanceof String)) {
      return object;
    }
    if (JAVA_INTEGER_TYPES.contains(type)) {
      return Integer.parseInt((String) object);
    } else if (JAVA_LONG_TYPES.contains(type)) {
      return Long.parseLong((String) object);
    } else if (FLOAT.equals(type)) {
      return Float.parseFloat((String) object);
    } else if (DOUBLE.equals(type)) {
      return Double.parseDouble((String) object);
    }
    return object;
  }

  public static <O> O cast(Object object) {
    @SuppressWarnings("unchecked")
    O result = (O) object;

    return result;
  }

  public static String humanReadableBytes(long bytes) {
    return humanReadableBytes(bytes, false);
  }

  public static String humanReadableBytes(long bytes, boolean si) {
    int unit = si ? 1000 : 1024;
    if (bytes < unit) {
      return bytes + " B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }

  public static Long maybeUndefined(long val) {
    return val == 4294967295L ? null : val;
  }

  /**
   * 对于null超过8个字段byte[]是LSB，即排序高的字段在低字节里面。
   *
   * 例如9个null字段，col按顺序的bitmap如下，表示
   * <pre>
   *   低位          高位
   *        col9  col2 col4
   *   [00000001][0,1,0,1,0,0,0,0]
   * </pre>
   */
  public static int[] getBitArray(SliceInput input, int numOfBits) {
    int size = (numOfBits + 7) / 8;
    input.decrPosition(size);
    byte[] byteArray = input.readByteArray(size);
    int[] result = new int[numOfBits];
    for (int i = 0; i < numOfBits; i++) {
      int idx = byteArray.length - 1 - i / 8;
      result[i] = ((byteArray[idx] >> (i % 8)) & 1);
    }
    return result;
  }

  public static <T> List<String> getFromBitArray(List<T> list, int[] bitArray, Function<T, String> func) {
    List<String> result = new ArrayList<>(bitArray.length);
    for (int i = 0; i < bitArray.length; i++) {
      if (bitArray[i] == 1) {
        result.add(func.apply(list.get(i)));
      }
    }
    return result;
  }

  public static int castCompare(Object recordKey, Object targetKey) {
    if (MAX.equals(recordKey)) {
      return 1;
    }
    if (MIN.equals(recordKey)) {
      return -1;
    }
    if (MAX.equals(targetKey)) {
      return -1;
    }
    if (MIN.equals(targetKey)) {
      return 1;
    }
    Comparable k1 = Utils.cast(recordKey);
    Comparable k2 = Utils.cast(targetKey);
    return k1.compareTo(k2);
  }

  public static void close(Closeable closeable) throws IOException {
    if (closeable == null) {
      return;
    }
    closeable.close();
  }

  /**
   * Use {@link StringBuilder} to build string out of an array.
   * <p/>
   * Sometimes by reusing StringBuilder, we can avoid creating many StringBuilder and good to garbage collection.
   *
   * @param a array
   * @param b reusable StringBuilder
   * @return array string
   */
  public static String arrayToString(Object[] a, StringBuilder b) {
    if (a == null) {
      return "null";
    }
    // clean StringBuilder
    b.delete(0, b.length());
    for (int i = 0; i < a.length; i++) {
      b.append(String.valueOf(a[i]));
      b.append(",");
    }
    if (b.length() > 0) {
      b.deleteCharAt(b.length() - 1);
    }
    return b.toString();
  }

}
