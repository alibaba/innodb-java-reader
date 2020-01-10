/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.util.SliceInput;

/**
 * AbstractColumnParser
 *
 * @author xu.zx
 */
public abstract class AbstractColumnParser<V> implements ColumnParser<V> {

  @Override
  public V readFrom(SliceInput input) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V readFrom(SliceInput input, int len, String charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V readFrom(SliceInput input, int len) {
    throw new UnsupportedOperationException();
  }

}
