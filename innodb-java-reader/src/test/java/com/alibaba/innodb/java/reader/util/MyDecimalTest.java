package com.alibaba.innodb.java.reader.util;

import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class MyDecimalTest {

  @Test
  public void testParseDecimal() {
    int precision = 18;
    int scale = 4;
    byte[] buf = {(byte) 0x80, 0x0, 0x1, 0xD, (byte) 0xFB, 0x38, (byte) 0xD2, 0x4, (byte) 0xD2};
    // byte[] buf = {127, -1, -2, -14, 4, -57, 45, -5, 45};
    MysqlDecimal myDecimal = new MysqlDecimal(precision, scale);
    myDecimal.parse(buf);
    System.out.println(myDecimal.toDecimal());
    assertThat(myDecimal.toDecimal(), is(new BigDecimal("1234567890.1234")));
  }

}
