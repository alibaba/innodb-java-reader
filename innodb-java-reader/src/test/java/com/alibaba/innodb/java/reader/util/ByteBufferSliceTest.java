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
  public void testSliceEquals() {
    ByteBuffer buffer1 = createByteBuffer();
    Slice slice1 = Slices.fromByteBuffer(buffer1);
    ByteBuffer buffer2 = createByteBuffer();
    Slice slice2 = Slices.fromByteBuffer(buffer2);
    assertThat(slice1.equals(slice2), is(true));
    System.out.println(slice1);
  }

  private ByteBuffer createByteBuffer() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(16384);
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
