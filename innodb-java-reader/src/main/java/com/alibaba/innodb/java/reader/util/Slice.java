/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import java.nio.ByteBuffer;

/**
 * Slice of a byte array or buffer.
 *
 * @author xu.zx
 */
public interface Slice {

  /**
   * Get length of this slice.
   *
   * @return length of the slice
   */
  int length();

  /**
   * Get the raw array of the slice.
   *
   * @return raw byte array
   */
  byte[] getRawArray();

  /**
   * Get the offset of the slice.
   *
   * @return raw offset
   */
  int getRawOffset();

  /**
   * Get a byte in the slice.
   *
   * @param index the specified absolute {@code index} in this slice
   * @return one byte
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   *                                   {@code index + 1} is greater than capacity
   */
  byte getByte(int index);

  /**
   * Get a 16-bit short integer in the slice.
   *
   * @param index the specified absolute {@code index} in this slice
   * @return 16-bit short integer
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   *                                   {@code index + 2} is greater than capacity
   */
  short getShort(int index);

  /**
   * Get a 24-bit integer in the slice.
   *
   * @param index the specified absolute {@code index} in this slice
   * @return 24-bit integer
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   *                                   {@code index + 3} is greater than capacity
   */
  int get3BytesInt(int index);

  /**
   * Get a 32-bit integer in the slice.
   *
   * @param index the specified absolute {@code index} in this slice.
   * @return 32-bit integer
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   *                                   {@code index + 4} is greater than capacity
   */
  int getInt(int index);

  /**
   * Get a 64-bit long integer in the slice.
   *
   * @param index the specified absolute {@code index} in this slice
   * @return 64-bit long integer
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   *                                   {@code index + 8} is greater than capacity
   */
  long getLong(int index);

  /**
   * Get 4 bytes float in the slice.
   *
   * @param index the specified absolute {@code index} in this slice
   * @return 4 bytes float
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   *                                   {@code index + 8} is greater than capacity
   */
  float getFloat(int index);

  /**
   * Get 8 bytes double in the slice.
   *
   * @param index the specified absolute {@code index} in this slice
   * @return 8 bytes double
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   *                                   {@code index + 8} is greater than capacity
   */
  double getDouble(int index);

  /**
   * Transfer this array or buffer to a byte array.
   *
   * @param index            the specified absolute {@code index} in this slice
   * @param destination      destination byte array
   * @param destinationIndex the first index of the destination
   * @param length           the number of bytes to transfer
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
   *                                   if the specified {@code dstIndex} is less than {@code 0},
   *                                   if {@code index + length} is greater than
   *                                   {@code this.capacity}, or
   *                                   if {@code dstIndex + length} is greater than
   *                                   {@code dst.length}
   */
  void getBytes(int index, byte[] destination, int destinationIndex, int length);

  /**
   * Get byte array of the slice.
   * Leave the implementation to decide to do deep copy or just a view.
   *
   * @return byte array
   */
  byte[] getBytes();

  /**
   * Get sub-region byte array of the slice.
   * Leave the implementation to decide to do deep copy or just a view.
   *
   * @param index  the specified absolute {@code index} in this slice
   * @param length length
   * @return byte array
   */
  byte[] getBytes(int index, int length);

  /**
   * Transfer this buffer's data to the specified destination starting at
   * the specified absolute {@code index} until the destination's position
   * reaches its limit.
   *
   * @param index       the specified absolute {@code index} in this slice
   * @param destination destination byte buffer
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   *                                   if {@code index + dst.remaining()} is greater than
   *                                   {@code this.capacity}
   */
  void getBytes(int index, ByteBuffer destination);

  /**
   * Create an input stream for this slice.
   *
   * @return SliceInput
   */
  default SliceInput input() {
    return new SliceInput(this);
  }
}
