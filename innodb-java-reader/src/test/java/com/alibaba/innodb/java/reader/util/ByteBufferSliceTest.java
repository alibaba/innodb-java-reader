package com.alibaba.innodb.java.reader.util;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ByteBufferSliceTest {

  @Test
  public void testSliceLength() {
    ByteBuffer buffer = createByteBuffer();
    Slice slice = Slices.fromByteBuffer(buffer);
    assertThat(slice.length(), is(16384));
  }

  @Test
  public void testSliceEmpty() {
    ByteBuffer buffer = ByteBuffer.allocate(0);
    Slice slice = Slices.fromByteBuffer(buffer);
    assertThat(slice.length(), is(0));
  }

  @Test
  public void testSliceGetBytes() {
    ByteBuffer buffer = createByteBuffer();
    Slice slice = Slices.fromByteBuffer(buffer);
    assertThat(slice.getBytes(), is(buffer.array()));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSliceGetRawArrayUnsupported() {
    ByteBuffer buffer = ByteBuffer.allocate(5);
    Slice slice = Slices.fromByteBuffer(buffer);
    slice.getRawArray();
  }

  @Test
  public void testSliceEquals() {
    ByteBuffer buffer1 = createByteBuffer();
    Slice slice1 = Slices.fromByteBuffer(buffer1);
    ByteBuffer buffer2 = createByteBuffer();
    Slice slice2 = Slices.fromByteBuffer(buffer2);
    assertThat(slice1.equals(slice2), is(true));
    assertThat(slice1.length(), is(16384));

  }

  private ByteBuffer createByteBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate(16384);
    buffer.position(1000);
    buffer.putLong(1000000L);
    buffer.position(5000);
    for (int i = 0; i < 100; i++) {
      buffer.putChar((char) ((97 + i) + i % 26));
    }
    buffer.position(10000);
    buffer.putInt(Integer.MAX_VALUE);
    return buffer;
  }

}
