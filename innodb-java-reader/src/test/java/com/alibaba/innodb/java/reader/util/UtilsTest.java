package com.alibaba.innodb.java.reader.util;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class UtilsTest {

  @Test
  public void testArrayToString() {
    Object[] array = new Object[5];
    array[0] = 1;
    array[1] = "abc";
    array[2] = 3.4;
    array[3] = 6.6D;
    array[4] = 10000000L;
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b), is("1,abc,3.4,6.6,10000000"));
    }
  }

  @Test
  public void testArrayToStringEmptyArray() {
    Object[] array = new Object[0];
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b), is(""));
    }
  }

  @Test
  public void testArrayToStringNullArray() {
    Object[] array = null;
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b), is("null"));
    }
  }

  @Test
  public void testArrayToStringOneElement() {
    Object[] array = new Object[1];
    array[0] = 1;
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b), is("1"));
    }
  }

  @Test
  public void testArrayToStringOneElementNull() {
    Object[] array = new Object[2];
    array[0] = 1;
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b), is("1,null"));
    }
  }

  @Test
  public void testArrayToStringTwoElementNull() {
    Object[] array = new Object[5];
    array[0] = 1;
    array[2] = "abc";
    array[4] = 0.0D;
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b), is("1,null,abc,null,0.0"));
    }
  }

}
