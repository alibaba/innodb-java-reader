/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import java.nio.ByteBuffer;

/**
 * Slices.
 *
 * @author xu.zx
 */
public final class Slices {

  private Slices() {
  }

  public static Slice fromByteBuffer(ByteBuffer buffer) {
    return new ByteBufferSlice(buffer);
  }

}
