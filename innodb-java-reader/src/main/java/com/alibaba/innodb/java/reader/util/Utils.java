/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import com.google.common.collect.ImmutableList;

import org.apache.commons.collections.CollectionUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.alibaba.innodb.java.reader.Constants.INT_10;
import static com.alibaba.innodb.java.reader.Constants.INT_100;
import static com.alibaba.innodb.java.reader.Constants.INT_1000;
import static com.alibaba.innodb.java.reader.Constants.INT_10000;
import static com.alibaba.innodb.java.reader.Constants.MAX_RECORD_1;
import static com.alibaba.innodb.java.reader.Constants.MAX_RECORD_2;
import static com.alibaba.innodb.java.reader.Constants.MAX_RECORD_3;
import static com.alibaba.innodb.java.reader.Constants.MAX_RECORD_4;
import static com.alibaba.innodb.java.reader.Constants.MAX_RECORD_5;
import static com.alibaba.innodb.java.reader.Constants.MAX_VAL;
import static com.alibaba.innodb.java.reader.Constants.MIN_RECORD_1;
import static com.alibaba.innodb.java.reader.Constants.MIN_RECORD_2;
import static com.alibaba.innodb.java.reader.Constants.MIN_RECORD_3;
import static com.alibaba.innodb.java.reader.Constants.MIN_RECORD_4;
import static com.alibaba.innodb.java.reader.Constants.MIN_RECORD_5;
import static com.alibaba.innodb.java.reader.Constants.MIN_VAL;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utils.
 *
 * @author xu.zx
 */
public class Utils {

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
   *
   * @param input     SliceInput
   * @param numOfBits number of bits
   * @return bit array
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

  public static <T> List<T> makeNotNull(T obj) {
    if (obj == null) {
      return ImmutableList.of();
    }
    return ImmutableList.of(obj);
  }

  public static <T> List<T> makeNotNull(List<T> list) {
    if (list == null) {
      return ImmutableList.of();
    }
    return list;
  }

  public static boolean allEmpty(Collection<?>... collections) {
    checkArgument(collections != null && collections.length > 0);
    for (Collection<?> collection : collections) {
      if (CollectionUtils.isNotEmpty(collection)) {
        return false;
      }
    }
    return true;
  }

  public static boolean noneEmpty(Collection<?>... collections) {
    checkArgument(collections != null && collections.length > 0);
    for (Collection<?> collection : collections) {
      if (CollectionUtils.isEmpty(collection)) {
        return false;
      }
    }
    return true;
  }

  public static boolean anyElementEmpty(Collection<?> collection) {
    checkArgument(collection != null);
    for (Object o : collection) {
      if (o == null) {
        return true;
      }
    }
    return false;
  }

  public static int castCompare(List<Object> recordKey, List<Object> targetKey) {
    if (recordKey.size() != targetKey.size()) {
      throw new IllegalStateException("Record key " + recordKey
          + " and target key " + targetKey + " length not match");
    }
    for (int i = 0; i < recordKey.size(); i++) {
      int res = castCompare(recordKey.get(i), targetKey.get(i));
      if (res != 0) {
        return res;
      }
    }
    return 0;
  }

  public static int castCompare(Object recordKey, Object targetKey) {
    if (MAX_VAL.equals(recordKey)) {
      return 1;
    }
    if (MIN_VAL.equals(recordKey)) {
      return -1;
    }
    if (MAX_VAL.equals(targetKey)) {
      return -1;
    }
    if (MIN_VAL.equals(targetKey)) {
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

  public static List<Object> constructMaxRecord(int keyLen) {
    checkArgument(keyLen > 0, "Key length should be bigger than 0");
    switch (keyLen) {
      case 1:
        return MAX_RECORD_1;
      case 2:
        return MAX_RECORD_2;
      case 3:
        return MAX_RECORD_3;
      case 4:
        return MAX_RECORD_4;
      case 5:
        return MAX_RECORD_5;
      default:
        List<Object> res = new ArrayList<>(keyLen);
        for (int i = 0; i < keyLen; i++) {
          res.add(MAX_VAL);
        }
        return Collections.unmodifiableList(res);
    }
  }

  public static List<Object> constructMinRecord(int keyLen) {
    checkArgument(keyLen > 0, "Key length should be bigger than 0");
    switch (keyLen) {
      case 1:
        return MIN_RECORD_1;
      case 2:
        return MIN_RECORD_2;
      case 3:
        return MIN_RECORD_3;
      case 4:
        return MIN_RECORD_4;
      case 5:
        return MIN_RECORD_5;
      default:
        List<Object> res = new ArrayList<>(keyLen);
        for (int i = 0; i < keyLen; i++) {
          res.add(MIN_VAL);
        }
        return Collections.unmodifiableList(res);
    }
  }

  /**
   * Use {@link StringBuilder} to build string out of an array.
   * <p>
   * Sometimes by reusing StringBuilder, we can avoid creating many StringBuilder and good to garbage collection.
   *
   * @param a         array
   * @param b         reusable StringBuilder
   * @param delimiter delimiter
   * @return array string
   */
  public static String arrayToString(Object[] a, StringBuilder b, String delimiter) {
    return arrayToString(a, b, delimiter, false);
  }

  /**
   * Use {@link StringBuilder} to build string out of an array.
   * <p>
   * Sometimes by reusing StringBuilder, we can avoid creating many StringBuilder and good to garbage collection.
   *
   * @param a         array
   * @param b         reusable StringBuilder
   * @param delimiter delimiter
   * @param newLine   if this is a new line, if true, write slash n at the end
   * @return array string
   */
  public static String arrayToString(Object[] a, StringBuilder b, String delimiter, boolean newLine) {
    if (a == null) {
      return "null";
    }
    // clean StringBuilder
    b.delete(0, b.length());
    for (int i = 0; i < a.length; i++) {
      b.append(a[i]);
      b.append(delimiter);
    }
    if (b.length() > 0) {
      b.deleteCharAt(b.length() - 1);
    }
    if (newLine) {
      b.append("\n");
    }
    return b.toString();
  }

  /**
   * Process file by dilimiter.
   *
   * @param filePath  file path
   * @param charset   encoding charset
   * @param func      consumer function to process each part separated by delimiter
   * @param delimiter delimiter
   * @return pair, left is success processing count, right is the last string read
   */
  public static Pair<Integer, String> processFileWithDelimiter(String filePath, String charset,
                                                               Consumer<String> func, String delimiter) {
    BufferedReader bufferedReader = null;
    try {
      int count = 0;
      bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
      String line;
      StringBuilder str = new StringBuilder();
      while ((line = bufferedReader.readLine()) != null) {
        if (line.contains(delimiter)) {
          int index = line.indexOf(delimiter);
          str.append(line.substring(0, index + 1));
          String content = str.toString();
          func.accept(content);
          str.delete(0, str.length());
          str.append(line.substring(index + 1));
          count++;
        } else {
          str.append(line);
        }
      }
      return Pair.of(count, str.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        if (bufferedReader != null) {
          bufferedReader.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static <T> boolean isOptionalPresent(Optional<T> optional) {
    return optional != null && optional.isPresent();
  }

  public static String tailTrim(String value) {
    int len = value.length();
    int st = 0;

    while ((st < len) && (value.charAt(len - 1) <= Symbol.SPACE_CHAR)) {
      len--;
    }
    return (len < value.length()) ? value.substring(0, len) : value;
  }

  public static String formatDate(int year, int month, int day) {
    return formatIntLessThan10000(year) + "-"
        + formatIntLessThan100(month) + "-"
        + formatIntLessThan100(day);
  }

  public static String formatIntLessThan10000(int val) {
    if (val >= INT_1000 && val < INT_10000) {
      return String.valueOf(val);
    } else if (val >= INT_100 && val < INT_1000) {
      return "0" + val;
    } else if (val >= INT_10 && val < INT_100) {
      return "00" + val;
    } else if (val >= 0 && val < INT_10) {
      return "000" + val;
    }
    throw new IllegalArgumentException("not possible");
  }

  public static String formatIntLessThan100(int val) {
    if (val >= INT_10 && val < INT_100) {
      return String.valueOf(val);
    } else if (val >= 0 && val < INT_10) {
      return "0" + val;
    }
    throw new IllegalArgumentException("not possible");
  }

}
