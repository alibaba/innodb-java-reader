package com.alibaba.innodb.java.reader.range;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderImpl;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class QueryIteratorMultipleLevelTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(false));
  }

  @Test
  public void testTableLevelMysql56() {
    testTableLevel(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
  }

  @Test
  public void testTableLevelMysql57() {
    testTableLevel(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb11.ibd");
  }

  public void testTableLevel(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      long numOfPages = reader.getNumOfPages();
      assertThat(numOfPages, greaterThan(500L));

      Index index = (Index) reader.readPage(3);
      assertThat(index.getIndexHeader().getPageLevel(), is(2));
    }
  }

  @Test
  public void testQueryAllIteratorMysql56() {
    testQueryAllIterator(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
  }

  @Test
  public void testQueryAllIteratorMysql57() {
    testQueryAllIterator(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb11.ibd");
  }

  public void testQueryAllIterator(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      Iterator<GenericRecord> iterator = reader.getQueryAllIterator();
      int count = 0;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        count++;
      }
      System.out.println(count);
      assertThat(count, is(40000));
    }
  }

  @Test
  public void testRangeQueryIteratorMysql56() {
    testRangeQueryIterator(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
  }

  @Test
  public void testRangeQueryIteratorMysql57() {
    testRangeQueryIterator(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb11.ibd");
  }

  public void testRangeQueryIterator(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(
          ImmutableList.of(10000), ComparisonOperator.GTE,
          ImmutableList.of(40000), ComparisonOperator.LT);
      int count = 0;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        count++;
      }
      System.out.println(count);
      assertThat(count, is(20000));
    }
  }

}
