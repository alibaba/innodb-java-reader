/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.cli.CsvPrinter;

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
    for (int i = 0; i < 10; i++) {
      assertThat(arrayToString(array, ","), is("1,abc,3.4,6.6,10000000"));
    }
  }

  @Test
  public void testArrayToStringEmptyArray() {
    Object[] array = new Object[0];
    for (int i = 0; i < 10; i++) {
      assertThat(arrayToString(array, ","), is(""));
    }
  }

  @Test
  public void testArrayToStringNullArray() {
    Object[] array = null;
    for (int i = 0; i < 10; i++) {
      assertThat(arrayToString(array, ","), is("null"));
    }
  }

  @Test
  public void testArrayToStringOneElement() {
    Object[] array = new Object[1];
    array[0] = 1;
    for (int i = 0; i < 10; i++) {
      assertThat(arrayToString(array, ","), is("1"));
    }
  }

  @Test
  public void testArrayToStringOneElementNull() {
    Object[] array = new Object[2];
    array[0] = 1;
    for (int i = 0; i < 10; i++) {
      assertThat(arrayToString(array, ","), is("1,null"));
    }
  }

  @Test
  public void testArrayToStringTwoElementNull() {
    Object[] array = new Object[5];
    array[0] = 1;
    array[2] = "abc";
    array[4] = 0.0D;
    for (int i = 0; i < 10; i++) {
      assertThat(arrayToString(array, ","), is("1,null,abc,null,0.0"));
    }
  }

  @Test
  public void testArrayToStringOneElementQuote() {
    Object[] array = new Object[1];
    array[0] = 1;
    for (int i = 0; i < 10; i++) {
      assertThat(arrayToString(array, "1", true), is("\"1\""));
    }
  }

  @Test
  public void testArrayToStringOneElementNullQuote() {
    Object[] array = new Object[2];
    array[0] = 1;
    for (int i = 0; i < 10; i++) {
      assertThat(arrayToString(array, ",", true), is("\"1\",null"));
    }
  }

  private static String arrayToString(Object[] a, String delimiter) {
    return arrayToString(a, delimiter, false);
  }

  private static String arrayToString(Object[] a, String delimiter, boolean quote) {
    return CsvPrinter.arrayToString(a, new StringBuilder(), delimiter, quote, false);
  }

}
