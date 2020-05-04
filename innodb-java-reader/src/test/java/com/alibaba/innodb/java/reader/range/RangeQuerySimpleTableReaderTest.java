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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class RangeQuerySimpleTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(true));
  }

  //==========================================================================
  // range query all
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryAllMysql56() {
    testSimpleTableRangeQueryAll(a -> a.withMysql56());
  }

  @Test
  public void testSimpleTableRangeQueryAllMysql57() {
    testSimpleTableRangeQueryAll(a -> a.withMysql57());
  }

  @Test
  public void testSimpleTableRangeQueryAllMysql80() {
    testSimpleTableRangeQueryAll(a -> a.withMysql80());
  }

  public void testSimpleTableRangeQueryAll(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(-1), ComparisonOperator.GTE,
            ImmutableList.of(20), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(Integer.MIN_VALUE), ComparisonOperator.GTE,
            ImmutableList.of(Integer.MAX_VALUE), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(0), ComparisonOperator.GTE,
            ImmutableList.of(11), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(1), ComparisonOperator.GTE,
            ImmutableList.of(11), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(-1), ComparisonOperator.GT,
            ImmutableList.of(11), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(-1), ComparisonOperator.GT,
            ImmutableList.of(10), ComparisonOperator.LTE);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(-1), ComparisonOperator.GTE,
            ImmutableList.of(11), ComparisonOperator.LT);
  }

  public Consumer<List<GenericRecord>> rangeQueryAllExpected() {
    return recordList -> {
      assertThat(recordList.size(), is(10));

      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assertThat(values[0], is(i));
        assertThat(values[1], is(i * 2L));
        assertThat(values[2], is(StringUtils.repeat('A', 16)));
        assertThat(values[3], is(StringUtils.repeat('C', 8) + (char) (97 + i)));
      }
    };
  }

  //==========================================================================
  // range query nothing
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryNothingMysql56() {
    testSimpleTableRangeQueryNothing(a -> a.withMysql56());
  }

  @Test
  public void testSimpleTableRangeQueryNothingMysql57() {
    testSimpleTableRangeQueryNothing(a -> a.withMysql57());
  }

  @Test
  public void testSimpleTableRangeQueryNothingMysql80() {
    testSimpleTableRangeQueryNothing(a -> a.withMysql80());
  }

  public void testSimpleTableRangeQueryNothing(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(),
            ImmutableList.of(-1), ComparisonOperator.GTE,
            ImmutableList.of(0), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(),
            ImmutableList.of(0), ComparisonOperator.GTE,
            ImmutableList.of(0), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(),
            ImmutableList.of(11), ComparisonOperator.GTE,
            ImmutableList.of(11), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(),
            ImmutableList.of(12), ComparisonOperator.GTE,
            ImmutableList.of(20), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(),
            ImmutableList.of(-1), ComparisonOperator.GT,
            ImmutableList.of(1), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(),
            ImmutableList.of(-1), ComparisonOperator.GTE,
            ImmutableList.of(-1), ComparisonOperator.LTE);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(),
            ImmutableList.of(-1), ComparisonOperator.GT,
            ImmutableList.of(-1), ComparisonOperator.LTE);
  }

  public Consumer<List<GenericRecord>> rangeQueryNothingExpected() {
    return recordList -> assertThat(recordList.size(), is(0));
  }

  //==========================================================================
  // range query partially
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryPartMysql56() {
    testSimpleTableRangeQueryPart(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd");
  }

  @Test
  public void testSimpleTableRangeQueryPartMysql57() {
    testSimpleTableRangeQueryPart(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd");
  }

  @Test
  public void testSimpleTableRangeQueryPartMysql80() {
    testSimpleTableRangeQueryPart(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd");
  }

  public void testSimpleTableRangeQueryPart(String path) {
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
    System.out.println(start + " " + end);
    List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
        ImmutableList.of(start), ComparisonOperator.GTE, ImmutableList.of(end), ComparisonOperator.LT);
    if (end == start) {
      end++;
    }
    assertThat(recordList.size(), is(end - start));
    int index = 0;
    for (int i = start; i < end; i++) {
      GenericRecord record = recordList.get(index++);
      Object[] values = record.getValues();
      System.out.println(Arrays.asList(values));
      assertThat(values[0], is(i));
      assertThat(values[1], is(i * 2L));
      assertThat(values[2], is(StringUtils.repeat('A', 16)));
      assertThat(values[3], is(StringUtils.repeat('C', 8) + (char) (97 + i)));
    }
  }

  //==========================================================================
  // range query illegal argument
  //==========================================================================

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleTableRangeQueryLowerUpperNotValid() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getTableDef())) {
      reader.open();
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(12), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LT);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleTableRangeQueryLowerUpperNotValid2() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getTableDef())) {
      reader.open();
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(12), ComparisonOperator.GTE,
          Utils.constructMinRecord(1), ComparisonOperator.LT);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleTableRangeQueryLowerUpperNotValid3() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getTableDef())) {
      reader.open();
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          Utils.constructMaxRecord(1), ComparisonOperator.GTE,
          ImmutableList.of(Integer.MIN_VALUE), ComparisonOperator.LT);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleTableRangeQueryLowerUpperNotValid4() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getTableDef())) {
      reader.open();
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(2), ComparisonOperator.GT,
          ImmutableList.of(1), ComparisonOperator.LT);
    }
  }

  //==========================================================================
  // range query half open and close
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryHalfOpenHalfCloseMysql56() {
    testSimpleTableRangeQueryHalfOpenHalfClose(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd");
  }

  @Test
  public void testSimpleTableRangeQueryHalfOpenHalfCloseMysql57() {
    testSimpleTableRangeQueryHalfOpenHalfClose(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd");
  }

  @Test
  public void testSimpleTableRangeQueryHalfOpenHalfCloseMysql80() {
    testSimpleTableRangeQueryHalfOpenHalfClose(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd");
  }

  public void testSimpleTableRangeQueryHalfOpenHalfClose(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      List<GenericRecord> recordList = null;

      for (int i = 0; i <= 10; i++) {
        recordList = reader.rangeQueryByPrimaryKey(
            ImmutableList.of(i), ComparisonOperator.GT,
            null, ComparisonOperator.NOP);
        assertThat(recordList.size(), is(10 - i));
      }

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(0), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(10));

      for (int i = 1; i <= 10; i++) {
        recordList = reader.rangeQueryByPrimaryKey(
            ImmutableList.of(i), ComparisonOperator.GTE,
            null, ComparisonOperator.NOP);
        assertThat(recordList.size(), is(10 - i + 1));
      }

      // max & min
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GTE,
          Utils.constructMaxRecord(1), ComparisonOperator.LT);
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GT,
          Utils.constructMaxRecord(1), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(9));

      recordList = reader.rangeQueryByPrimaryKey(
          Utils.constructMinRecord(1), ComparisonOperator.GTE,
          ImmutableList.of(8), ComparisonOperator.LT);
      assertThat(recordList.size(), is(7));

      recordList = reader.rangeQueryByPrimaryKey(
          Utils.constructMinRecord(1), ComparisonOperator.GT,
          ImmutableList.of(8), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(8));

      // mid, side by side
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(4), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(2));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(4), ComparisonOperator.LT);
      assertThat(recordList.size(), is(1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(4), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(4), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      // mid, gap
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(3));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(2));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(2));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(1));

      // mid, gap
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(8), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(6));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(8), ComparisonOperator.LT);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(8), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(8), ComparisonOperator.LT);
      assertThat(recordList.size(), is(4));

      // mid, end
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(10), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(8));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(12), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(8));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(10), ComparisonOperator.LT);
      assertThat(recordList.size(), is(7));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GTE,
          ImmutableList.of(11), ComparisonOperator.LT);
      assertThat(recordList.size(), is(8));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(10), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(7));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(11), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(7));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(10), ComparisonOperator.LT);
      assertThat(recordList.size(), is(6));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(3), ComparisonOperator.GT,
          ImmutableList.of(11), ComparisonOperator.LT);
      assertThat(recordList.size(), is(7));

      // mid, start
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(0), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(4));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(-1), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(4));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GT,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(4));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(0), ComparisonOperator.GT,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GT,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(3));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(0), ComparisonOperator.GT,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(4));

      // same element starts
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GT,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GT,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));
      // same element ends

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(100), ComparisonOperator.LT);
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(10), ComparisonOperator.LT);
      assertThat(recordList.size(), is(9));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(10), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(6), ComparisonOperator.LT);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          new ArrayList<>(), ComparisonOperator.NOP,
          ImmutableList.of(6), ComparisonOperator.LT);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          Utils.constructMinRecord(1), ComparisonOperator.GTE,
          ImmutableList.of(6), ComparisonOperator.LT);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(1), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(1));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(5), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(1), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(5), ComparisonOperator.LT);
      assertThat(recordList.size(), is(4));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(10));
    }
  }

  //==========================================================================
  // range query with predicate
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryWithRecordPredicate() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", getTableDef())) {
      reader.open();

      Predicate<GenericRecord> predicate = r -> (long) (r.get("a")) == 12L;

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(4), ComparisonOperator.GTE,
          ImmutableList.of(9), ComparisonOperator.LT,
          predicate);
      assertThat(recordList.size(), is(1));
      assertThat(recordList.get(0).getPrimaryKey(), is(ImmutableList.of(6)));
      assertThat(recordList.get(0).get("a"), is(12L));

      predicate = r -> (long) (r.get("a")) == 99999L;

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(4), ComparisonOperator.GTE,
          ImmutableList.of(9), ComparisonOperator.LT,
          predicate);
      assertThat(recordList.size(), is(0));

      predicate = r -> {
        return (long) (r.get("a")) == 12L && (long) (r.get("a")) == 10L;
      };

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(4), ComparisonOperator.GTE,
          ImmutableList.of(9), ComparisonOperator.LT,
          predicate);
      assertThat(recordList.size(), is(0));

      predicate = r -> {
        return (long) (r.get("a")) == 12L || (long) (r.get("a")) == 10L;
      };

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(4), ComparisonOperator.GTE,
          ImmutableList.of(9), ComparisonOperator.LT,
          predicate);
      assertThat(recordList.size(), is(2));

      predicate = r -> (long) (r.get("a")) == 12L;

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(4), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LTE,
          predicate);
      assertThat(recordList.size(), is(0));
    }
  }

  //==========================================================================
  // range query with projection
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryWithProjection() {
    String path = IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd";
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("id"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("id", "b"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("id", "b", "a"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("a"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("b"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("c"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("a", "b"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("a", "c"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("b", "c"));
    doTestSimpleTableRangeQueryWithProjection(path, ImmutableList.of("a", "b", "c"));
  }

  public void doTestSimpleTableRangeQueryWithProjection(String path, List<String> projection) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(4), ComparisonOperator.GT,
          ImmutableList.of(9), ComparisonOperator.LTE,
          projection);
      int index = 0;
      for (int i = 5; i <= 9; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        // pk should always present
        assertThat(record.get("id"), is(i));
        assertThat(record.get("a"), is(projection.contains("a") ? i * 2L : null));
        assertThat(record.get("b"), is(projection.contains("b") ? StringUtils.repeat('A', 16) : null));
        assertThat(record.get("c"), is(projection.contains("c")
            ? StringUtils.repeat('C', 8) + (char) (97 + i) : null));
      }
    }
  }

  //==========================================================================
  // range query with predicate and projection
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryWithPredicateProjection() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", getTableDef())) {
      reader.open();

      Predicate<GenericRecord> predicate = r -> (long) (r.get("a")) == 12L;

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(4), ComparisonOperator.GTE,
          ImmutableList.of(9), ComparisonOperator.LT,
          predicate, ImmutableList.of("a"));
      assertThat(recordList.size(), is(1));
      assertThat(recordList.get(0).getPrimaryKey(), is(ImmutableList.of(6)));
      assertThat(recordList.get(0).get("a"), is(12L));
      assertThat(recordList.get(0).get("b"), nullValue());
      assertThat(recordList.get(0).get("c"), nullValue());
    }
  }

  //==========================================================================
  // others
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryOther() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", getTableDef())) {
      reader.open();

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(0), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GTE,
          ImmutableList.of(8), ComparisonOperator.LT);
      assertThat(recordList.size(), is(7));
    }
  }


}
