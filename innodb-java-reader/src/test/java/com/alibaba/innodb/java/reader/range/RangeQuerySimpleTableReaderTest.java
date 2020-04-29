package com.alibaba.innodb.java.reader.range;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderImpl;
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
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(-1), ImmutableList.of(20));

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(Integer.MIN_VALUE), ImmutableList.of(Integer.MAX_VALUE));

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(0), ImmutableList.of(11));

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(1), ImmutableList.of(11));
  }

  @Test
  public void testSimpleTableRangeQueryAllMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(-1), ImmutableList.of(20));

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(Integer.MIN_VALUE), ImmutableList.of(Integer.MAX_VALUE));

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(0), ImmutableList.of(11));

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(1), ImmutableList.of(11));
  }

  @Test
  public void testSimpleTableRangeQueryAllMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(-1), ImmutableList.of(20));

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(Integer.MIN_VALUE), ImmutableList.of(Integer.MAX_VALUE));

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(0), ImmutableList.of(11));

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryAllExpected(),
            ImmutableList.of(1), ImmutableList.of(11));
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
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(-1), ImmutableList.of(0));

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(0), ImmutableList.of(0));

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(11), ImmutableList.of(11));

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(12), ImmutableList.of(20));
  }

  @Test
  public void testSimpleTableRangeQueryNothingMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(-1), ImmutableList.of(0));

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(0), ImmutableList.of(0));

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(11), ImmutableList.of(11));

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(12), ImmutableList.of(20));
  }

  @Test
  public void testSimpleTableRangeQueryNothingMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(-1), ImmutableList.of(0));

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(0), ImmutableList.of(0));

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(11), ImmutableList.of(11));

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryRecordsIs(rangeQueryNothingExpected(), ImmutableList.of(12), ImmutableList.of(20));
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
        ImmutableList.of(start), ImmutableList.of(end));
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
          ImmutableList.of(12), ImmutableList.of(5));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleTableRangeQueryLowerUpperNotValid2() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getTableDef())) {
      reader.open();
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(12), Utils.constructMinRecord(1));
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

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(ImmutableList.of(0), null);
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(ImmutableList.of(1), null);
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(ImmutableList.of(1), Utils.constructMaxRecord(1));
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(ImmutableList.of(5), null);
      assertThat(recordList.size(), is(6));

      recordList = reader.rangeQueryByPrimaryKey(ImmutableList.of(5), new ArrayList<>());
      assertThat(recordList.size(), is(6));

      recordList = reader.rangeQueryByPrimaryKey(null, ImmutableList.of(100));
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(null, ImmutableList.of(6));
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(new ArrayList<>(), ImmutableList.of(6));
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(Utils.constructMinRecord(1), ImmutableList.of(6));
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(null, ImmutableList.of(1));
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(null, null);
      assertThat(recordList.size(), is(10));
    }
  }

  //==========================================================================
  // others
  //==========================================================================

  @Test
  public void testSimpleTableRangeQueryOther() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", getTableDef())) {
      reader.open();

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(ImmutableList.of(0), null);
      assertThat(recordList.size(), is(10));

      recordList = reader.rangeQueryByPrimaryKey(ImmutableList.of(1), ImmutableList.of(8));
      assertThat(recordList.size(), is(7));
    }
  }

  @Test
  public void testSimpleTableRangeQueryWithRecordPredicate() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", getTableDef())) {
      reader.open();

      Predicate<GenericRecord> predicate = r -> (long) (r.get("a")) == 12L;

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(4), ImmutableList.of(9), predicate);
      assertThat(recordList.size(), is(1));
      assertThat(recordList.get(0).getPrimaryKey(), is(ImmutableList.of(6)));
      assertThat(recordList.get(0).get("a"), is(12L));
    }
  }
}
