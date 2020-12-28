package com.alibaba.innodb.java.reader.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Inflater;

/**
 * @author jie xu
 * @description
 * @created 2020/12/22
 **/
public class ZlibUtil {
  private static final int BUF_SIZE = 1024;
  public static byte[] decompress(byte[] data) {
    Inflater decompresser = new Inflater();
    decompresser.reset();
    decompresser.setInput(data);
    ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
    try {
      byte[] buf = new byte[BUF_SIZE];
      while (!decompresser.finished()) {
        int i = decompresser.inflate(buf);
        o.write(buf, 0, i);
      }
      return o.toByteArray();
    } catch (Exception e) {
        throw new RuntimeException(e);
    } finally {
      try {
        decompresser.end();
        o.close();
      } catch (IOException ignored) {

      }
    }
  }
}
