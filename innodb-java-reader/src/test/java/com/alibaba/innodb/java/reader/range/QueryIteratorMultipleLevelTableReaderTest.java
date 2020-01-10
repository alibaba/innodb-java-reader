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
public class QueryIteratorMultipleLevelTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(false));
  }

  @Test
  public void testQueryAllIterator() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
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
  public void testRangeQueryIterator() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
      reader.open();

      Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(10000, 40000);
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
