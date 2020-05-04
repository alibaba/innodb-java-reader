/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.util.SliceInput;

/**
 * Abstract column parser.
 *
 * @author xu.zx
 */
public abstract class AbstractColumnParser<V> implements ColumnParser<V> {

  @Override
  public V readFrom(SliceInput input, int len, String charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V readFrom(SliceInput input, Column column) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipFrom(SliceInput input, int len, String charset) {
    readFrom(input, len, charset);
  }

  @Override
  public void skipFrom(SliceInput input, Column column) {
    readFrom(input, column);
  }

  @Override
  public Class<?> typeClass() {
    throw new UnsupportedOperationException();
  }

}
