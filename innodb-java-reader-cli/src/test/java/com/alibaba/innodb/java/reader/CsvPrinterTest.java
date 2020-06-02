package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.cli.CsvPrinter;
import com.alibaba.innodb.java.reader.cli.QuoteMode;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * CSV printer test.
 *
 * @author xu.zx
 * @author Adam Jurcik
 */
public class CsvPrinterTest {

  @Test
  public void testArrayToString() {
    Object[] array = new Object[5];
    array[0] = 1;
    array[1] = "abc";
    array[2] = 3.4;
    array[3] = 6.6D;
    array[4] = 10000000L;
    CsvPrinter p = defaultPrinter();
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("1,abc,3.4,6.6,10000000"));
    }
  }

  @Test
  public void testArrayToStringEmptyArray() {
    Object[] array = new Object[0];
    CsvPrinter p = defaultPrinter();
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is(""));
    }
  }

  @Test
  public void testArrayToStringNullArray() {
    Object[] array = null;
    CsvPrinter p = defaultPrinter();
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("null"));
    }
  }

  @Test
  public void testArrayToStringOneElement() {
    Object[] array = new Object[1];
    array[0] = 1;
    CsvPrinter p = defaultPrinter();
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("1"));
    }
  }

  @Test
  public void testArrayToStringOneElementNull() {
    Object[] array = new Object[2];
    array[0] = 1;
    CsvPrinter p = defaultPrinter();
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("1,null"));
    }
  }

  @Test
  public void testArrayToStringTwoElementNull() {
    Object[] array = new Object[5];
    array[0] = 1;
    array[2] = "abc";
    array[4] = 0.0D;
    CsvPrinter p = defaultPrinter();
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("1,null,abc,null,0.0"));
    }
  }

  @Test
  public void testArrayToStringQuoteAll() {
    Object[] array = new Object[6];
    array[0] = 1;
    array[1] = "abc";
    array[2] = 3.4;
    array[3] = 6.6D;
    array[4] = 10000000L;
    array[5] = null;
    CsvPrinter p = new CsvPrinter(",", QuoteMode.ALL, "null");
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("\"1\",\"abc\",\"3.4\",\"6.6\",\"10000000\",\"null\""));
    }
  }

  @Test
  public void testArrayToStringQuoteNonNull() {
    Object[] array = new Object[6];
    array[0] = 1;
    array[1] = "abc";
    array[2] = 3.4;
    array[3] = 6.6D;
    array[4] = 10000000L;
    array[5] = null;
    CsvPrinter p = new CsvPrinter(",", QuoteMode.NON_NULL, "null");
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("\"1\",\"abc\",\"3.4\",\"6.6\",\"10000000\",null"));
    }
  }

  @Test
  public void testArrayToStringQuoteNonNumeric() {
    Object[] array = new Object[6];
    array[0] = 1;
    array[1] = "abc";
    array[2] = 3.4;
    array[3] = 6.6D;
    array[4] = 10000000L;
    array[5] = null;
    CsvPrinter p = new CsvPrinter(",", QuoteMode.NON_NUMERIC, "null");
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("1,\"abc\",3.4,6.6,10000000,null"));
    }
  }

  @Test
  public void testArrayToStringQuoteNone() {
    Object[] array = new Object[6];
    array[0] = 1;
    array[1] = "abc";
    array[2] = 3.4;
    array[3] = 6.6D;
    array[4] = 10000000L;
    array[5] = null;
    CsvPrinter p = new CsvPrinter(",", QuoteMode.NONE, "null");
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("1,abc,3.4,6.6,10000000,null"));
    }
  }

  @Test
  public void testArrayToStringNullString() {
    Object[] array = new Object[1];
    array[0] = null;
    CsvPrinter p = new CsvPrinter(",", QuoteMode.NONE, "NULL");
    for (int i = 0; i < 10; i++) {
      assertThat(p.arrayToString(array, false), is("NULL"));
    }
  }

  private static CsvPrinter defaultPrinter() {
    return new CsvPrinter(",", QuoteMode.NONE, "null");
  }

}
