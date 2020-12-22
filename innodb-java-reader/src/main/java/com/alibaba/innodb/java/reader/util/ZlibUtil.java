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
  public static byte[] decompress(byte[] data) {
    byte[] output = new byte[0];

    Inflater decompresser = new Inflater();
    decompresser.reset();
    decompresser.setInput(data);

    ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
    try {
      byte[] buf = new byte[1024];
      while (!decompresser.finished()) {
        int i = decompresser.inflate(buf);
        o.write(buf, 0, i);
      }
      output = o.toByteArray();
    } catch (Exception e) {
      output = data;
      e.printStackTrace();
    } finally {
      try {
        o.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    decompresser.end();
    return output;
  }
}
