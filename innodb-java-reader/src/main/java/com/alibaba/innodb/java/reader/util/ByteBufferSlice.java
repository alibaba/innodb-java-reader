/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_BYTE;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_INT;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_LONG;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_MEDIUMINT;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_SHORT;
import static java.util.Objects.requireNonNull;

/**
 * Slice of a {@link ByteBuffer}.
 * <p/>
 * This class is not thread-safe.
 *
 * @author xu.zx
 */
public final class ByteBufferSlice implements Slice {

  private final ByteBuffer data;

  private final int offset;

  private final int length;

  private int hash;

  public ByteBufferSlice(ByteBuffer buffer) {
    this(buffer, 0, buffer.capacity());
  }

  public ByteBufferSlice(ByteBuffer buffer, int offset, int length) {
    requireNonNull(buffer, "buffer is null");
    this.data = buffer;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public byte[] getRawArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRawOffset() {
    return offset;
  }

  @Override
  public byte getByte(int index) {
    checkPositionIndexes(index, index + SIZE_OF_BYTE, this.length);
    index += offset;
    return data.get(index);
  }

  @Override
  public short getShort(int index) {
    checkPositionIndexes(index, index + SIZE_OF_SHORT, this.length);
    index += offset;
    return (short) (data.get(index) << 8 | data.get(index + 1) & 255);
  }

  @Override
  public int get3BytesInt(int index) {
    checkPositionIndexes(index, index + SIZE_OF_MEDIUMINT, this.length);
    index += offset;
    return (data.get(index + 2) & 0xff)
        | (data.get(index + 1) & 0xff) << 8
        | (data.get(index) & 0xff) << 16;
  }

  @Override
  public int getInt(int index) {
    checkPositionIndexes(index, index + SIZE_OF_INT, this.length);
    index += offset;
    return (data.get(index + 3) & 0xff)
        | (data.get(index + 2) & 0xff) << 8
        | (data.get(index + 1) & 0xff) << 16
        | (data.get(index) & 0xff) << 24;
  }

  @Override
  public long getLong(int index) {
    checkPositionIndexes(index, index + SIZE_OF_LONG, this.length);
    index += offset;
    return ((long) data.get(index + 7) & 0xff)
        | ((long) data.get(index + 6) & 0xff) << 8
        | ((long) data.get(index + 5) & 0xff) << 16
        | ((long) data.get(index + 4) & 0xff) << 24
        | ((long) data.get(index + 3) & 0xff) << 32
        | ((long) data.get(index + 2) & 0xff) << 40
        | ((long) data.get(index + 1) & 0xff) << 48
        | ((long) data.get(index) & 0xff) << 56;
  }

  @Override
  public float getFloat(int index) {
    checkPositionIndexes(index, index + SIZE_OF_INT, this.length);
    index += offset;
    data.position(index);
    return data.order(ByteOrder.LITTLE_ENDIAN).getFloat();
  }

  @Override
  public double getDouble(int index) {
    checkPositionIndexes(index, index + SIZE_OF_LONG, this.length);
    index += offset;
    data.position(index);
    return data.order(ByteOrder.LITTLE_ENDIAN).getDouble();
  }

  @Override
  public void getBytes(int index, byte[] destination, int destinationIndex, int length) {
    checkPositionIndexes(index, index + length, this.length);
    checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);
    index += offset;
    data.position(index);
    data.get(destination, destinationIndex, length);
  }

  @Override
  public byte[] getBytes() {
    return getBytes(0, length);
  }

  @Override
  public byte[] getBytes(int index, int length) {
    byte[] value = new byte[length];
    getBytes(index, value, 0, length);
    return value;
  }

  @Override
  public void getBytes(int index, ByteBuffer destination) {
    checkPositionIndex(index, this.length);
    index += offset;
    byte[] bytes = getBytes(index, Math.min(length, destination.remaining()));
    destination.put(bytes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ByteBufferSlice slice = (ByteBufferSlice) o;

    // do lengths match
    if (length != slice.length) {
      return false;
    }

    // if arrays have same base offset, some optimizations can be taken...
    if (offset == slice.offset && data == slice.data) {
      return true;
    }
    for (int i = 0; i < length; i++) {
      if (data.get(offset + i) != slice.data.get(slice.offset + i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (hash != 0) {
      return hash;
    }

    int result = length;
    for (int i = offset; i < offset + length; i++) {
      result = 31 * result + data.get(i);
    }
    if (result == 0) {
      result = 1;
    }
    hash = result;
    return hash;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + '(' + "length=" + length() + ')';
  }
}
