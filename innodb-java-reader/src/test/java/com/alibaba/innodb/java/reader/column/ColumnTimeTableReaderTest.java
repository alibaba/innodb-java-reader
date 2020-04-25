package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * <pre>
 * mysql> select * from tb03\G;
 * *************************** 1. row ***************************
 * id: 1
 *  a: 100
 *  b: 2019-10-02 10:59:59
 *  c: 2019-10-02 02:59:59
 *  d: 10:59:59
 * *************************** 2. row ***************************
 * id: 2
 *  a: 101
 *  b: 1970-01-01 08:00:01
 *  c: 1970-01-01 00:00:01
 *  d: 08:00:01
 * *************************** 3. row ***************************
 * id: 3
 *  a: 102
 *  b: 2008-11-23 09:23:00
 *  c: 2008-11-23 01:23:00
 *  d: 09:23:00
 * </pre>
 *
 * @author xu.zx
 */
public class ColumnTimeTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("int(11)").setNullable(false))
        .addColumn(new Column().setName("b").setType("datetime").setNullable(false))
        .addColumn(new Column().setName("c").setType("timestamp").setNullable(false))
        .addColumn(new Column().setName("d").setType("time").setNullable(false));
  }

  @Test
  public void testTimeColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testTimeColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testTimeColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(3));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(1));
      assertThat(r1.get("a"), is(100));
      assertThat(r1.get("b"), is("2019-10-02 10:59:59"));
      assertThat(r1.get("c"), is("1569985199"));
      assertThat(r1.get("d"), is("10:59:59"));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(2));
      assertThat(r2.get("a"), is(101));
      assertThat(r2.get("b"), is("1970-01-01 08:00:01"));
      assertThat(r2.get("c"), is("1"));
      assertThat(r2.get("d"), is("08:00:01"));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey(), is(3));
      assertThat(r3.get("a"), is(102));
      assertThat(r3.get("b"), is("2008-11-23 09:23:00"));
      assertThat(r3.get("c"), is("1227403380"));
      assertThat(r3.get("d"), is("09:23:00"));
    };
  }
}
