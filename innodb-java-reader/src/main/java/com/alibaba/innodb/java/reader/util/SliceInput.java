package com.alibaba.innodb.java.reader.util;

import com.alibaba.innodb.java.reader.exception.ReaderException;

import java.io.DataInput;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * SliceInput
 *
 * @author xu.zx
 */
public final class SliceInput extends InputStream implements DataInput {

  /**
   * The max ulonglong - 0x ff ff ff ff ff ff ff ff
   */
  private static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551615");

  private final Slice slice;

  private int position;

  public SliceInput(Slice slice) {
    this.slice = slice;
  }

  public int position() {
    return position;
  }

  public void setPosition(int position) {
    if (position < 0 || position > slice.length()) {
      throw new IndexOutOfBoundsException();
    }
    this.position = position;
  }

  public int incPosition(int delta) {
    if (position + delta > slice.length()) {
      throw new IndexOutOfBoundsException();
    }
    this.position += delta;
    return this.position;
  }

  public int decrPosition(int delta) {
    if (position - delta < 0) {
      throw new IndexOutOfBoundsException();
    }
    this.position -= delta;
    return this.position;
  }

  @Override
  public int available() {
    return slice.length() - position;
  }

  @Override
  public boolean readBoolean() {
    return readByte() != 0;
  }

  @Override
  public int read() {
    return readByte();
  }

  @Override
  public byte readByte() {
    return slice.getByte(position++);
  }

  @Override
  public int readUnsignedByte() {
    return (short) (readByte() & 0xff);
  }

  @Override
  public short readShort() {
    short v = slice.getShort(position);
    position += 2;
    return v;
  }

  @Override
  public int readUnsignedShort() {
    return readShort() & 0xffff;
  }

  public int read3BytesInt() {
    int v = slice.get3BytesInt(position);
    position += 3;
    return v;
  }

  public int readUnsigned3BytesInt() {
    return read3BytesInt() & 0xffffff;
  }

  @Override
  public int readInt() {
    int v = slice.getInt(position);
    position += 4;
    return v;
  }

  public long readUnsignedInt() {
    return readInt() & 0xffffffffL;
  }

  @Override
  public long readLong() {
    long v = slice.getLong(position);
    position += 8;
    return v;
  }

  public BigInteger readUnsignedLong() {
    long v = slice.getLong(position);
    position += 8;
    return (v >= 0) ? BigInteger.valueOf(v) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + v));
  }

  @Override
  public float readFloat() {
    float v = slice.getFloat(position);
    position += 4;
    return v;
  }

  @Override
  public double readDouble() {
    double v = slice.getDouble(position);
    position += 8;
    return v;
  }

  public String readUTF8String(int length) {
    return readString(length, "UTF-8");
  }

  public String readString(int length, String charset) {
    // deep copy is preferred here
    byte[] value = slice.getBytes(position, length);
    position += length;
    try {
      return new String(value, charset);
    } catch (UnsupportedEncodingException e) {
      throw new ReaderException(e);
    }
  }

  public byte[] readByteArray(int length) {
    // deep copy is preferred here
    byte[] value = slice.getBytes(position, length);
    position += length;
    return value;
  }

  @Override
  public void readFully(byte[] destination) {
    readBytes(destination);
  }

  public void readBytes(byte[] destination) {
    readBytes(destination, 0, destination.length);
  }

  @Override
  public void readFully(byte[] destination, int offset, int length) {
    readBytes(destination, offset, length);
  }

  public void readBytes(byte[] destination, int destinationIndex, int length) {
    slice.getBytes(position, destination, destinationIndex, length);
    position += length;
  }

  /**
   * Transfers this buffer's data to the specified destination starting at
   * the current {@code position} until the destination's position
   * reaches its limit, and increases the {@code position} by the
   * number of the transferred bytes.
   *
   * @param destination destination
   * @throws IndexOutOfBoundsException if {@code destination.remaining()} is greater than
   *                                   {@code this.available()}
   */
  public void readBytes(ByteBuffer destination) {
    int length = destination.remaining();
    slice.getBytes(position, destination);
    position += length;
  }

  @Override
  public int skipBytes(int length) {
    int len = Math.min(length, available());
    position += len;
    return len;
  }

  /**
   * static uint64 unpack_bigendian(const uchar* d, uint len)
   * {
   * assert(len <= 8);
   * uint64 val = 0;
   * int i = (int)len;
   * int s = 0;
   * while (i != 0)
   * {
   * i--;
   * uint64 v = d[i];
   * val += (v << s);
   * s += 8;
   * }
   * return val;
   * }
   */
  public long unpackBigendian(int len) {
    byte[] bytes = readByteArray(len);
    checkArgument(len <= 8);
    long val = 0;
    int i = len;
    int s = 0;
    while (i != 0) {
      i--;
      long v = (bytes[i] & 0xffL);
      val += (v << s);
      s += 8;
    }
    return val;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + '(' + "pos=" + position + ", " + "cap=" + slice.length() + ')';
  }

  //
  // Unsupported operations
  //

  /**
   * Unsupported operation
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public char readChar() {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported operation
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public String readLine() {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported operation
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public String readUTF() {
    throw new UnsupportedOperationException();
  }
}
