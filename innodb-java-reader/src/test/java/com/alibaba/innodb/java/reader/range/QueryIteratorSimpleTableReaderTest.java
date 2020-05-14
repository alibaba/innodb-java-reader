package com.alibaba.innodb.java.reader.range;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderImpl;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.util.Utils;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class QueryIteratorSimpleTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(true));
  }

  //==========================================================================
  // queryAllIterator test
  //==========================================================================

  @Test
  public void testQueryAllIteratorMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkQueryAllIterator(testQueryAllIteratorExpected());
  }

  @Test
  public void testQueryAllIteratorMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkQueryAllIterator(testQueryAllIteratorExpected());
  }

  @Test
  public void testQueryAllIteratorMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkQueryAllIterator(testQueryAllIteratorExpected());
  }

  public Consumer<Iterator<GenericRecord>> testQueryAllIteratorExpected() {
    return iterator -> {

      int count = 0;
      int i = 1;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();

        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assertThat(values[0], is(i));
        assertThat(values[1], is(i * 2L));
        assertThat(values[2], is(StringUtils.repeat('A', 16)));
        assertThat(values[3], is(StringUtils.repeat('C', 8) + (char) (97 + i)));

        assertThat(record.getPrimaryKey(), is(ImmutableList.of(i)));
        assertThat(record.get(0), is(i));
        assertThat(record.get("id"), is(i));

        assertThat(record.get(1), is(i * 2L));
        assertThat(record.get("a"), is(i * 2L));

        assertThat(record.get(2), is(StringUtils.repeat('A', 16)));
        assertThat(record.get("b"), is(StringUtils.repeat('A', 16)));

        assertThat(record.get(3), is(StringUtils.repeat('C', 8) + (char) (97 + i)));
        assertThat(record.get("c"), is(StringUtils.repeat('C', 8) + (char) (97 + i)));

        i++;
        count++;
      }
      System.out.println(count);
      assertThat(count, is(10));
      assertThat(iterator.hasNext(), is(false));
    };
  }

  //==========================================================================
  // queryAllIterator with projection test
  //==========================================================================

  @Test
  public void testQueryAllIteratorWithProjectionMysql56() {
    testQueryAllIteratorWithProjection(a -> a.withMysql56());
  }

  @Test
  public void testQueryAllIteratorWithProjectionMysql57() {
    testQueryAllIteratorWithProjection(a -> a.withMysql57());
  }

  @Test
  public void testQueryAllIteratorWithProjectionMysql80() {
    testQueryAllIteratorWithProjection(a -> a.withMysql80());
  }

  public void testQueryAllIteratorWithProjection(Consumer<AssertThat> func) {
    List<List<String>> params = Arrays.asList(
        ImmutableList.of("id"),
        ImmutableList.of("id", "b"),
        ImmutableList.of("id", "b", "a"),
        ImmutableList.of("a"),
        ImmutableList.of("b"),
        ImmutableList.of("c"),
        ImmutableList.of("a", "b"),
        ImmutableList.of("a", "c"),
        ImmutableList.of("b", "c"),
        ImmutableList.of("a", "b", "c")
    );

    for (List<String> param : params) {
      AssertThat assertThat = assertTestOf(this);
      func.accept(assertThat);
      assertThat.withTableDef(getTableDef())
          .checkQueryAllIteratorProjection(testQueryAllIteratorWithProjectionExpected(param), param);
    }
  }

  public Consumer<Iterator<GenericRecord>> testQueryAllIteratorWithProjectionExpected(List<String> projection) {
    return iterator -> {

      int count = 0;
      int i = 1;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();

        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assertThat(values[0], is(i));
        assertThat(values[1], is(projection.contains("a") ? i * 2L : null));
        assertThat(values[2], is(projection.contains("b") ? StringUtils.repeat('A', 16) : null));
        assertThat(values[3], is(projection.contains("c")
            ? StringUtils.repeat('C', 8) + (char) (97 + i) : null));

        assertThat(record.getPrimaryKey(), is(ImmutableList.of(i)));
        assertThat(record.get(0), is(i));
        assertThat(record.get("id"), is(i));

        assertThat(record.get(1), is(projection.contains("a") ? i * 2L : null));
        assertThat(record.get("a"), is(projection.contains("a") ? i * 2L : null));

        assertThat(record.get(2), is(projection.contains("b") ? StringUtils.repeat('A', 16) : null));
        assertThat(record.get("b"), is(projection.contains("b") ? StringUtils.repeat('A', 16) : null));

        assertThat(record.get(3), is(projection.contains("c")
            ? StringUtils.repeat('C', 8) + (char) (97 + i) : null));
        assertThat(record.get("c"), is(projection.contains("c")
            ? StringUtils.repeat('C', 8) + (char) (97 + i) : null));

        i++;
        count++;
      }
      System.out.println(count);
      assertThat(count, is(10));
      assertThat(iterator.hasNext(), is(false));
    };
  }

  //==========================================================================
  // range query nothing
  //==========================================================================

  @Test
  public void testRangeQueryIteratorNothingMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(-1), ComparisonOperator.GTE,
            ImmutableList.of(0), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(0), ComparisonOperator.GTE,
            ImmutableList.of(0), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(11), ComparisonOperator.GTE,
            ImmutableList.of(11), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(12), ComparisonOperator.GTE,
            ImmutableList.of(20), ComparisonOperator.LT);
  }

  @Test
  public void testRangeQueryIteratorNothingMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(-1), ComparisonOperator.GTE,
            ImmutableList.of(0), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(0), ComparisonOperator.GTE,
            ImmutableList.of(0), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(11), ComparisonOperator.GTE,
            ImmutableList.of(11), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(12), ComparisonOperator.GTE,
            ImmutableList.of(20), ComparisonOperator.LT);
  }

  @Test
  public void testRangeQueryIteratorNothingMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(-1), ComparisonOperator.GTE,
            ImmutableList.of(0), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(0), ComparisonOperator.GTE,
            ImmutableList.of(0), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(11), ComparisonOperator.GTE,
            ImmutableList.of(11), ComparisonOperator.LT);

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of(12), ComparisonOperator.GTE,
            ImmutableList.of(20), ComparisonOperator.LT);
  }

  public Consumer<Iterator<GenericRecord>> rangeQueryNothingExpected() {
    return iterator -> assertThat(iterator.hasNext(), is(false));
  }

  //==========================================================================
  // range query partially
  //==========================================================================

  @Test
  public void testRangeQueryIteratorPartMysql56() {
    testRangeQueryPart(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd");
  }

  @Test
  public void testRangeQueryIteratorPartMysql57() {
    testRangeQueryPart(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd");
  }

  @Test
  public void testRangeQueryIteratorPartMysql80() {
    testRangeQueryPart(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd");
  }

  public void testRangeQueryPart(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();
      for (int i = 1; i <= 10; i++) {
        for (int j = i; j <= 10; j++) {
          rangeQuery(reader, i, j);
        }
      }
    }
  }

  private void rangeQuery(TableReader reader, int start, int end) {
    rangeQuery(reader, start, end, ComparisonOperator.GTE, ComparisonOperator.LT);
    rangeQuery(reader, start, end, ComparisonOperator.GTE, ComparisonOperator.LTE);
    rangeQuery(reader, start, end, ComparisonOperator.GT, ComparisonOperator.LTE);
    rangeQuery(reader, start, end, ComparisonOperator.GT, ComparisonOperator.LT);
  }

  private void rangeQuery(TableReader reader, int start, int end,
                          ComparisonOperator lowerOp, ComparisonOperator upperOp) {
    Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(
        ImmutableList.of(start), lowerOp, ImmutableList.of(end), upperOp);
    int expectedSize = end - start;
    if (expectedSize > 0
        && lowerOp == ComparisonOperator.GT && upperOp == ComparisonOperator.LT) {
      expectedSize--;
    }
    if (lowerOp == ComparisonOperator.GTE && upperOp == ComparisonOperator.LTE) {
      expectedSize++;
    }
    int count = 0;
    int i = lowerOp == ComparisonOperator.GTE ? start : start + 1;
    while (iterator.hasNext()) {
      GenericRecord record = iterator.next();
      Object[] values = record.getValues();
      System.out.println(Arrays.asList(values));
      assertThat(values[0], is(i));
      assertThat(values[1], is(i * 2L));
      assertThat(values[2], is(StringUtils.repeat('A', 16)));
      assertThat(values[3], is(StringUtils.repeat('C', 8) + (char) (97 + i)));
      count++;
      i++;
    }

    assertThat(count, is(expectedSize));
    assertThat(iterator.hasNext(), is(false));
  }

  //==========================================================================
  // range query half open and close
  //==========================================================================

  @Test
  public void testRangeQueryIteratorHalfOpenHalfCloseMysql56() {
    testRangeQueryHalfOpenHalfClose(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd");
  }

  @Test
  public void testRangeQueryIteratorHalfOpenHalfCloseMysql57() {
    testRangeQueryHalfOpenHalfClose(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd");
  }

  @Test
  public void testRangeQueryIteratorHalfOpenHalfCloseMysql80() {
    testRangeQueryHalfOpenHalfClose(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd");
  }

  public void testRangeQueryHalfOpenHalfClose(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(
          ImmutableList.of(0), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(getIteratorSize(iterator), is(10));

      iterator = reader.getRangeQueryIterator(
          ImmutableList.of(1), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(getIteratorSize(iterator), is(10));

      iterator = reader.getRangeQueryIterator(
          ImmutableList.of(5), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(getIteratorSize(iterator), is(6));

      iterator = reader.getRangeQueryIterator(
          ImmutableList.of(5), ComparisonOperator.GTE,
          Utils.constructMaxRecord(1), ComparisonOperator.LT);
      assertThat(getIteratorSize(iterator), is(6));

      iterator = reader.getRangeQueryIterator(
          null, ComparisonOperator.NOP,
          ImmutableList.of(100), ComparisonOperator.LT);
      assertThat(getIteratorSize(iterator), is(10));

      iterator = reader.getRangeQueryIterator(
          null, ComparisonOperator.NOP,
          ImmutableList.of(6), ComparisonOperator.LT);
      assertThat(getIteratorSize(iterator), is(5));

      iterator = reader.getRangeQueryIterator(
          Utils.constructMinRecord(1), ComparisonOperator.GTE,
          ImmutableList.of(6), ComparisonOperator.LT);
      assertThat(getIteratorSize(iterator), is(5));

      iterator = reader.getRangeQueryIterator(
          null, ComparisonOperator.NOP,
          ImmutableList.of(1), ComparisonOperator.LT);
      assertThat(getIteratorSize(iterator), is(0));

      iterator = reader.getRangeQueryIterator(
          null, ComparisonOperator.NOP,
          null, ComparisonOperator.NOP);
      assertThat(getIteratorSize(iterator), is(10));
    }
  }

  //==========================================================================
  // rangeQueryAllIterator with projection test
  //==========================================================================

  @Test
  public void testRangeQueryIteratorWithProjectionMysql56() {
    testRangeQueryIteratorWithProjection(a -> a.withMysql56());
  }

  @Test
  public void testRangeQueryIteratorWithProjectionMysql57() {
    testRangeQueryIteratorWithProjection(a -> a.withMysql57());
  }

  @Test
  public void testRangeQueryIteratorWithProjectionMysql80() {
    testRangeQueryIteratorWithProjection(a -> a.withMysql80());
  }

  public void testRangeQueryIteratorWithProjection(Consumer<AssertThat> func) {
    List<List<String>> params = Arrays.asList(
        ImmutableList.of("id"),
        ImmutableList.of("id", "b"),
        ImmutableList.of("id", "b", "a"),
        ImmutableList.of("a"),
        ImmutableList.of("b"),
        ImmutableList.of("c"),
        ImmutableList.of("a", "b"),
        ImmutableList.of("a", "c"),
        ImmutableList.of("b", "c"),
        ImmutableList.of("a", "b", "c")
    );

    for (List<String> param : params) {
      AssertThat assertThat = assertTestOf(this);
      func.accept(assertThat);
      assertThat.withTableDef(getTableDef())
          .checkRangeQueryIteratorProjection(testRangeQueryIteratorWithProjectionExpected(param),
              ImmutableList.of(5), ComparisonOperator.GTE,
              ImmutableList.of(8), ComparisonOperator.LT,
              param);
    }
  }

  public Consumer<Iterator<GenericRecord>> testRangeQueryIteratorWithProjectionExpected(List<String> projection) {
    return iterator -> {

      int count = 0;
      int i = 5;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();

        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assertThat(values[0], is(i));
        assertThat(values[1], is(projection.contains("a") ? i * 2L : null));
        assertThat(values[2], is(projection.contains("b") ? StringUtils.repeat('A', 16) : null));
        assertThat(values[3], is(projection.contains("c")
            ? StringUtils.repeat('C', 8) + (char) (97 + i) : null));

        assertThat(record.getPrimaryKey(), is(ImmutableList.of(i)));
        assertThat(record.get(0), is(i));
        assertThat(record.get("id"), is(i));

        assertThat(record.get(1), is(projection.contains("a") ? i * 2L : null));
        assertThat(record.get("a"), is(projection.contains("a") ? i * 2L : null));

        assertThat(record.get(2), is(projection.contains("b") ? StringUtils.repeat('A', 16) : null));
        assertThat(record.get("b"), is(projection.contains("b") ? StringUtils.repeat('A', 16) : null));

        assertThat(record.get(3), is(projection.contains("c")
            ? StringUtils.repeat('C', 8) + (char) (97 + i) : null));
        assertThat(record.get("c"), is(projection.contains("c")
            ? StringUtils.repeat('C', 8) + (char) (97 + i) : null));

        i++;
        count++;
      }
      System.out.println(count);
      assertThat(count, is(3));
      assertThat(iterator.hasNext(), is(false));
    };
  }

  private int getIteratorSize(Iterator<GenericRecord> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertThat(iterator.hasNext(), is(false));
    return count;
  }

}
