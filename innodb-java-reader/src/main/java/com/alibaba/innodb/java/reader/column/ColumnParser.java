/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.util.SliceInput;

/**
 * Column parser
 *
 * @author xu.zx
 */
public interface ColumnParser<V> {

  /**
   * Read value from byte array input
   *
   * @param input slice input
   * @return value
   */
  V readFrom(SliceInput input);

  /**
   * Read value from byte array input with length
   *
   * @param input slice input
   * @param len   length
   * @return value
   */
  V readFrom(SliceInput input, int len);

  /**
   * Read value from byte array input with length and charset
   *
   * @param input   slice input
   * @param len     length
   * @param charset charset
   * @return value
   */
  V readFrom(SliceInput input, int len, String charset);

  /**
   * Returned value class
   *
   * @return class
   */
  Class<?> typeClass();

}
