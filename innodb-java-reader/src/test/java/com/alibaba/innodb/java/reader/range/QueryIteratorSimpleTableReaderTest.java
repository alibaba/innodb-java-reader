package com.alibaba.innodb.java.reader.range;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Test;

import java.util.Iterator;

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
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(false));
  }

  @Test
  public void testQueryAllIteratorMysql56() {
    testQueryAllIterator(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd");
  }

  @Test
  public void testQueryAllIteratorMysql57() {
    testQueryAllIterator(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd");
  }

  @Test
  public void testQueryAllIteratorMysql80() {
    testQueryAllIterator(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd");
  }

  public void testQueryAllIterator(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      Iterator<GenericRecord> iterator = reader.getQueryAllIterator();
      int count = 0;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        count++;
      }
      System.out.println(count);
      assertThat(count, is(10));
    }
  }

  @Test
  public void testRangeQueryIteratorMysql56() {
    testRangeQueryIterator(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd");
  }

  @Test
  public void testRangeQueryIteratorMysql57() {
    testRangeQueryIterator(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd");
  }

  @Test
  public void testRangeQueryIteratorMysql80() {
    testRangeQueryIterator(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd");
  }

  public void testRangeQueryIterator(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(5, 8);
      int count = 0;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        count++;
      }
      System.out.println(count);
      assertThat(count, is(3));
    }
  }

}
