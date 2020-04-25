package com.alibaba.innodb.java.reader.range;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class QueryIteratorSimpleTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
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
        .withSchema(getSchema())
        .checkQueryAllIterator(testQueryAllIteratorExpected());
  }

  @Test
  public void testQueryAllIteratorMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkQueryAllIterator(testQueryAllIteratorExpected());
  }

  @Test
  public void testQueryAllIteratorMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
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

        assertThat(record.getPrimaryKey(), is(i));
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
    };
  }

  //==========================================================================
  // range query nothing
  //==========================================================================

  @Test
  public void testRangeQueryIteratorNothingMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), -1, 0);

    assertTestOf(this)
        .withMysql56()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 0, 0);

    assertTestOf(this)
        .withMysql56()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 11, 11);

    assertTestOf(this)
        .withMysql56()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 12, 20);
  }

  @Test
  public void testRangeQueryIteratorNothingMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), -1, 0);

    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 0, 0);

    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 11, 11);

    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 12, 20);
  }

  @Test
  public void testRangeQueryIteratorNothingMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), -1, 0);

    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 0, 0);

    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 11, 11);

    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
        .checkRangeQueryIterator(rangeQueryNothingExpected(), 12, 20);
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
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();
      rangeQuery(reader, 1, 7);
      rangeQuery(reader, 1, 9);
      rangeQuery(reader, 2, 4);
      rangeQuery(reader, 3, 8);
      rangeQuery(reader, 5, 7);
      rangeQuery(reader, 6, 6);
    }
  }

  private void rangeQuery(TableReader reader, int start, int end) {
    Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(start, end);
    if (end == start) {
      end++;
    }

    int count = 0;
    int i = start;
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

    assertThat(count, is(end - start));
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
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(0, null);
      assertThat(getIteratorSize(iterator), is(10));

      iterator = reader.getRangeQueryIterator(1, null);
      assertThat(getIteratorSize(iterator), is(10));

      iterator = reader.getRangeQueryIterator(5, null);
      assertThat(getIteratorSize(iterator), is(6));

      iterator = reader.getRangeQueryIterator(null, 100);
      assertThat(getIteratorSize(iterator), is(10));

      iterator = reader.getRangeQueryIterator(null, 6);
      assertThat(getIteratorSize(iterator), is(5));

      iterator = reader.getRangeQueryIterator(null, 1);
      assertThat(getIteratorSize(iterator), is(0));

      iterator = reader.getRangeQueryIterator(null, null);
      assertThat(getIteratorSize(iterator), is(10));
    }
  }

  private int getIteratorSize(Iterator<GenericRecord> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    return count;
  }

}
