package com.alibaba.innodb.java.reader;

import java.nio.ByteBuffer;

/**
 * @author xu.zx
 */
public class AbstractTest {

  public static final String IBD_FILE_BASE_PATH_MYSQL56 = "src/test/resources/testsuite/mysql56/";
  public static final String IBD_FILE_BASE_PATH_MYSQL57 = "src/test/resources/testsuite/mysql57/";
  public static final String IBD_FILE_BASE_PATH_MYSQL80 = "src/test/resources/testsuite/mysql80/";
  public static final String IBD_FILE_BASE_PATH = IBD_FILE_BASE_PATH_MYSQL56;

  protected byte[] getContent(byte prefix, byte b, int repeatB) {
    return getContent(prefix, b, repeatB, repeatB + 1);
  }

  protected byte[] getContent(byte prefix, byte b, int repeatB, int len) {
    ByteBuffer buffer = ByteBuffer.allocate(len);
    buffer.put(prefix);
    for (int i = 0; i < repeatB; i++) {
      buffer.put(b);
    }
    if (buffer.remaining() > 0) {
      for (int i = 0; i < buffer.remaining(); i++) {
        buffer.put((byte) 0x00);
      }
    }
    return buffer.array();
  }

}
