package com.alibaba.innodb.java.reader.util;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.innodb.java.reader.Constants.MAX_VAL;
import static com.alibaba.innodb.java.reader.Constants.MIN_VAL;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
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
      assertThat(Utils.arrayToString(array, b, ","), is("1,abc,3.4,6.6,10000000"));
    }
  }

  @Test
  public void testArrayToStringEmptyArray() {
    Object[] array = new Object[0];
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b, ","), is(""));
    }
  }

  @Test
  public void testArrayToStringNullArray() {
    Object[] array = null;
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b, ","), is("null"));
    }
  }

  @Test
  public void testArrayToStringOneElement() {
    Object[] array = new Object[1];
    array[0] = 1;
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b, ","), is("1"));
    }
  }

  @Test
  public void testArrayToStringOneElementNull() {
    Object[] array = new Object[2];
    array[0] = 1;
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      assertThat(Utils.arrayToString(array, b, ","), is("1,null"));
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
      assertThat(Utils.arrayToString(array, b, ","), is("1,null,abc,null,0.0"));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCastCompareLenNotMatch() {
    List<Object> o1 = new ArrayList<>();
    o1.add("abc");
    o1.add("bcd");
    List<Object> o2 = new ArrayList<>();
    o2.add("xyz");
    Utils.castCompare(o1, o2);
  }

  @Test(expected = IllegalStateException.class)
  public void testCastCompareLenNotMatch2() {
    List<Object> o1 = new ArrayList<>();
    o1.add("abc");
    o1.add("bcd");
    List<Object> o2 = new ArrayList<>();
    o2.add(Utils.constructMinRecord(1));
    Utils.castCompare(o1, o2);
  }

  @Test(expected = ClassCastException.class)
  public void testCastCompareLenClassMatch() {
    List<Object> o1 = new ArrayList<>();
    o1.add("abc");
    List<Object> o2 = new ArrayList<>();
    o2.add(1);
    Utils.castCompare(o1, o2);
  }

  @Test(expected = ClassCastException.class)
  public void testCastCompareLenClassMatch2() {
    List<Object> o1 = new ArrayList<>();
    o1.add(12.45);
    List<Object> o2 = new ArrayList<>();
    o2.add(10L);
    Utils.castCompare(o1, o2);
  }

  @Test(expected = ClassCastException.class)
  public void testCastCompareLenClassMatch3() {
    List<Object> o1 = new ArrayList<>();
    o1.add(10);
    List<Object> o2 = new ArrayList<>();
    o2.add(10L);
    Utils.castCompare(o1, o2);
  }

  @Test
  public void testCastCompareOneColumn() {
    List<Object> o1 = new ArrayList<>();
    List<Object> o2 = new ArrayList<>();
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = ImmutableList.of();
    o2 = ImmutableList.of();
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = new ArrayList<>();
    o2 = ImmutableList.of();
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = ImmutableList.of(1);
    o2 = ImmutableList.of(2);
    assertThat(Utils.castCompare(o1, o2), is(-1));

    o1 = ImmutableList.of(100);
    o2 = ImmutableList.of(100);
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = ImmutableList.of(100);
    o2 = ImmutableList.of(50);
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = ImmutableList.of("1");
    o2 = ImmutableList.of("2");
    assertThat(Utils.castCompare(o1, o2), is(-1));

    o1 = ImmutableList.of("100");
    o2 = ImmutableList.of("100");
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = ImmutableList.of("100");
    o2 = ImmutableList.of("50");
    assertThat(Utils.castCompare(o1, o2), is(-4));

    o1 = ImmutableList.of("100");
    o2 = Utils.constructMaxRecord(1);
    assertThat(Utils.castCompare(o1, o2), is(-1));

    o1 = ImmutableList.of("100");
    o2 = Utils.constructMinRecord(1);
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = Utils.constructMaxRecord(1);
    o2 = ImmutableList.of("100");
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = Utils.constructMinRecord(1);
    o2 = ImmutableList.of("100");
    assertThat(Utils.castCompare(o1, o2), is(-1));

    o1 = ImmutableList.of(true);
    o2 = ImmutableList.of(false);
    assertThat(Utils.castCompare(o1, o2), is(1));

  }

  @Test
  public void testCastCompareCompositeColumn() {
    List<Object> o1 = new ArrayList<>();
    o1.add(1);
    o1.add(2);
    List<Object> o2 = new ArrayList<>();
    o2.add(1);
    o2.add(3);
    assertThat(Utils.castCompare(o1, o2), is(-1));

    o1 = ImmutableList.of(100, 200);
    o2 = ImmutableList.of(1000, 2000);
    assertThat(Utils.castCompare(o1, o2), is(-1));

    o1 = ImmutableList.of("hello", 200);
    o2 = ImmutableList.of("Hello", 2000);
    assertThat(Utils.castCompare(o1, o2), greaterThan(0));

    o1 = ImmutableList.of("hello", 200);
    o2 = ImmutableList.of("hello", 2000);
    assertThat(Utils.castCompare(o1, o2), lessThan(0));

    o1 = ImmutableList.of(100, 200);
    o2 = ImmutableList.of(100, 200);
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = ImmutableList.of("zhang", "xu");
    o2 = ImmutableList.of("zhang", "xu");
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = ImmutableList.of(1000, 2000);
    o2 = ImmutableList.of(100, 200);
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = ImmutableList.of("Hello", 2000);
    o2 = ImmutableList.of("hello", 200);
    assertThat(Utils.castCompare(o1, o2), lessThan(0));

    o1 = ImmutableList.of("hello", 2000);
    o2 = ImmutableList.of("hello", 200);
    assertThat(Utils.castCompare(o1, o2), greaterThan(0));

    o1 = ImmutableList.of("hello", "world", "abc");
    o2 = ImmutableList.of("hello", "world", "bcd");
    assertThat(Utils.castCompare(o1, o2), lessThan(0));

    o1 = ImmutableList.of("hello", "world", "abc");
    o2 = ImmutableList.of("hello", "world", "abc");
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = ImmutableList.of("hello", "world", "bcd");
    o2 = ImmutableList.of("hello", "world", "abc");
    assertThat(Utils.castCompare(o1, o2), greaterThan(0));

    o1 = ImmutableList.of(100, 12.45);
    o2 = ImmutableList.of(100, 99d);
    assertThat(Utils.castCompare(o1, o2), lessThan(0));

    o1 = ImmutableList.of(100, 12.45);
    o2 = ImmutableList.of(100, 12.45);
    assertThat(Utils.castCompare(o1, o2), is(0));

    o1 = ImmutableList.of(100, 99.0);
    o2 = ImmutableList.of(100, 12.45);
    assertThat(Utils.castCompare(o1, o2), greaterThan(0));

    o1 = ImmutableList.of(100, new Integer(5));
    o2 = ImmutableList.of(100, 6);
    assertThat(Utils.castCompare(o1, o2), lessThan(0));

    o1 = ImmutableList.of(100, new Long(900L));
    o2 = ImmutableList.of(100, new Long(1000L));
    assertThat(Utils.castCompare(o1, o2), lessThan(0));

    o1 = ImmutableList.of(100, new Long(900L));
    o2 = ImmutableList.of(100, new Long(1000L));
    assertThat(Utils.castCompare(o1, o2), lessThan(0));

    o1 = ImmutableList.of(100, new Double(0.0));
    o2 = ImmutableList.of(100, new Double(0.0));
    assertThat(Utils.castCompare(o1, o2), is(0));

    // who comes first is bigger
    o1 = ImmutableList.of(100, MAX_VAL);
    o2 = ImmutableList.of(100, MAX_VAL);
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = ImmutableList.of(100, MAX_VAL);
    o2 = ImmutableList.of(100, 20);
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = ImmutableList.of(100, MAX_VAL);
    o2 = ImmutableList.of(100, Long.MAX_VALUE);
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = ImmutableList.of(100, MAX_VAL);
    o2 = ImmutableList.of(100, MIN_VAL);
    assertThat(Utils.castCompare(o1, o2), is(1));

    // who comes first is smaller
    o1 = ImmutableList.of(100, MIN_VAL);
    o2 = ImmutableList.of(100, MIN_VAL);
    assertThat(Utils.castCompare(o1, o2), is(-1));

    o1 = ImmutableList.of(100, 20);
    o2 = ImmutableList.of(100, MIN_VAL);
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = ImmutableList.of(100, Integer.MIN_VALUE);
    o2 = ImmutableList.of(100, MIN_VAL);
    assertThat(Utils.castCompare(o1, o2), is(1));

    o1 = ImmutableList.of(100, MIN_VAL);
    o2 = ImmutableList.of(100, MAX_VAL);
    assertThat(Utils.castCompare(o1, o2), is(-1));
  }

  @Test
  public void testProcessFileWithDelimiter() {
    String filePath = "src/test/resources/test.sql";
    String delimiter = ";";
    List<String> list = new ArrayList<>();
    Utils.processFileWithDelimiter(filePath, "UTF-8", s -> {
      if (s.startsWith("CREATE TABLE")) {
        list.add(s.trim());
      }
    }, delimiter);
    assertThat(list.isEmpty(), is(false));
    for (String s : list) {
      System.out.println(s);
      assertThat(s.startsWith("CREATE TABLE"), is(true));
      assertThat(s.endsWith(";"), is(true));
    }
  }

}
