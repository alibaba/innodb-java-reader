package com.alibaba.innodb.java.reader.range;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderImpl;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.util.Utils;

import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class RangeQueryMultipleLevelTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(false));
  }

  @Ignore
  @Test
  public void testReadAllPages() {
    try (TableReader reader = new TableReaderImpl(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getTableDef())) {
      reader.open();

      // check read all pages function
      List<AbstractPage> pages = reader.readAllPages();
      for (AbstractPage page : pages) {
        StringBuilder sb = new StringBuilder();
        sb.append(page.getPageNumber()).append(" ");
        sb.append(page.getFilHeader()).append(" ");
        if (page instanceof Index) {
          sb.append(((Index) page).getIndexHeader());
        }
        System.out.println(sb.toString());
      }
    }
  }

  //==========================================================================
  // range query all
  //==========================================================================

  @Test
  public void testMultipleLevelTableRangeQueryAllMysql56() {
    testMultipleLevelTableRangeQueryAll(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
  }

  @Test
  public void testMultipleLevelTableRangeQueryAllMysql57() {
    testMultipleLevelTableRangeQueryAll(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb11.ibd");
  }

  public void testMultipleLevelTableRangeQueryAll(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(0), ComparisonOperator.GTE,
          ImmutableList.of(50001), ComparisonOperator.LT);
      assertThat(recordList.size(), is(40000));
      checkRecords(recordList.subList(0, 20000), 1, 20000);
      //  1 - 20000, 30001 - 50000
      // there is gap in the middle
      checkRecords(recordList.subList(20000, 40000), 30001, 50000);
    }
  }

  //==========================================================================
  // range query nothing
  //==========================================================================

  @Test
  public void testMultipleLevelTableRangeQueryNothingMysql56() {
    testMultipleLevelTableRangeQueryNothing(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
  }

  @Test
  public void testMultipleLevelTableRangeQueryNothingMysql57() {
    testMultipleLevelTableRangeQueryNothing(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb11.ibd");
  }

  public void testMultipleLevelTableRangeQueryNothing(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(-1), ComparisonOperator.GTE,
          ImmutableList.of(0), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(0), ComparisonOperator.GTE,
          ImmutableList.of(0), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(40000), ComparisonOperator.GT,
          ImmutableList.of(40000), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(50001), ComparisonOperator.GTE,
          ImmutableList.of(50001), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(50001), ComparisonOperator.GT,
          ImmutableList.of(50001), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(50001), ComparisonOperator.GTE,
          ImmutableList.of(60001), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(42000), ComparisonOperator.GTE,
          ImmutableList.of(41999), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(42000), ComparisonOperator.GTE,
          ImmutableList.of(0), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(0));
    }
  }

  //==========================================================================
  // range query partially
  //==========================================================================

  @Test
  public void testMultipleLevelTableRangeQueryPartMysql56() {
    testMultipleLevelTableRangeQueryPart(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
  }

  @Test
  public void testMultipleLevelTableRangeQueryPartMysql57() {
    testMultipleLevelTableRangeQueryPart(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb11.ibd");
  }

  public void testMultipleLevelTableRangeQueryPart(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();
      rangeQuery(reader, 5000, 5001);

      rangeQuery(reader, 3000, 8000);

      rangeQuery(reader, 5000, 16900);

      rangeQuery(reader, 10000, 19999);

      rangeQuery(reader, 42000, 47653);

      rangeQuery(reader, 42000, 42000);
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
    List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
        ImmutableList.of(start), lowerOp, ImmutableList.of(end), upperOp);
    int expectedSize = end - start;
    if (expectedSize > 0
        && lowerOp == ComparisonOperator.GT && upperOp == ComparisonOperator.LT) {
      expectedSize--;
    }
    if (lowerOp == ComparisonOperator.GTE && upperOp == ComparisonOperator.LTE) {
      expectedSize++;
    }
    assertThat(recordList.size(), is(expectedSize));
    int index = 0;
    int iStart = lowerOp == ComparisonOperator.GTE ? start : start + 1;
    int iEnd = upperOp == ComparisonOperator.LTE ? end : end - 1;
    for (int i = iStart; i <= iEnd; i++) {
      GenericRecord record = recordList.get(index++);
      Object[] values = record.getValues();
      //System.out.println(Arrays.asList(values));
      assertThat(values[0], is(i));
      assertThat(record.get("a"), is(i * 2L));
      assertThat(record.get("b"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 32))));
      assertThat(record.get("c"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 512))));
    }
  }

  //==========================================================================
  // range query half open and close
  //==========================================================================

  @Test
  public void testRangeQueryHalfOpenHalfCloseMysql56() {
    testRangeQueryHalfOpenHalfClose(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
  }

  @Test
  public void testRangeQueryHalfOpenHalfCloseMysql57() {
    testRangeQueryHalfOpenHalfClose(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb11.ibd");
  }

  public void testRangeQueryHalfOpenHalfClose(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(0), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(40000));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(40000));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GT,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(39999));

      // 39999 - 50000
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(39999), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(10002));

      // 40000 - 50000
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(39999), ComparisonOperator.GT,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(10001));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(40000 - 5 + 1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GT,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(40000 - 5));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GTE,
          new ArrayList<>(), ComparisonOperator.NOP);
      assertThat(recordList.size(), is(40000 - 5 + 1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(16500), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(40000 - 16500 + 1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(16500), ComparisonOperator.GT,
          null, ComparisonOperator.NOP);
      assertThat(recordList.size(), is(40000 - 16500));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(16500), ComparisonOperator.GTE,
          Utils.constructMaxRecord(1), ComparisonOperator.LT);
      assertThat(recordList.size(), is(40000 - 16500 + 1));

      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(16500), ComparisonOperator.GT,
          Utils.constructMaxRecord(1), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(40000 - 16500));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(100), ComparisonOperator.LT);
      assertThat(recordList.size(), is(99));

      recordList = reader.rangeQueryByPrimaryKey(
          new ArrayList<>(), ComparisonOperator.NOP,
          ImmutableList.of(51), ComparisonOperator.LT);
      assertThat(recordList.size(), is(50));

      recordList = reader.rangeQueryByPrimaryKey(
          new ArrayList<>(), ComparisonOperator.NOP,
          ImmutableList.of(51), ComparisonOperator.LTE);
      assertThat(recordList.size(), is(51));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(6), ComparisonOperator.LT);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(
          Utils.constructMinRecord(1), ComparisonOperator.GTE,
          ImmutableList.of(7), ComparisonOperator.LT);
      assertThat(recordList.size(), is(6));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(1), ComparisonOperator.LT);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(
          null, ComparisonOperator.NOP,
          ImmutableList.of(16500), ComparisonOperator.LT);
      assertThat(recordList.size(), is(16499));
    }
  }

  private void checkRecords(List<GenericRecord> recordList, int start, int end) {
    int index = 0;
    for (int i = start; i <= end; i++) {
      GenericRecord record = recordList.get(index++);
      Object[] values = record.getValues();
      //System.out.println(Arrays.asList(values));
      assertThat(values[0], is(i));
      assertThat(record.get("a"), is(i * 2L));
      assertThat(record.get("b"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 32))));
      assertThat(record.get("c"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 512))));
    }
  }
}
