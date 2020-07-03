package com.alibaba.innodb.java.reader.util;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class Ut0Crc32Test {

  @Test
  public void testCrc32() {
    String str = "innodb";
    byte[] bytes = str.getBytes();
    assertThat(Ut0Crc32.crc32(bytes, 0, bytes.length), is(1090276284L));
    assertThat(Ut0Crc32.crc32ByteByByte(bytes, 0, bytes.length), is(1090276284L));
  }

}
