package com.alibaba.innodb.java.reader.column;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/***
 * @author xu.zx
 */
public class ColumnTimeFractionalSecondsTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("int(11)").setNullable(false))
        .addColumn(new Column().setName("b").setType("datetime(3)").setNullable(false))
        .addColumn(new Column().setName("c").setType("datetime(6)").setNullable(false))
        .addColumn(new Column().setName("d").setType("timestamp(6)").setNullable(false))
        .addColumn(new Column().setName("e").setType("time(5)").setNullable(false))
        .addColumn(new Column().setName("f").setType("datetime(0)").setNullable(false));
  }

  @Test
  public void testTimeFractionalSecondsColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testTimeFractionalSecondsColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testTimeFractionalSecondsColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(3));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(ImmutableList.of(1)));
      assertThat(r1.get("a"), is(100));
      assertThat(r1.get("b"), is("2019-10-02 10:59:59.123"));
      assertThat(r1.get("c"), is("2000-01-01 00:01:03.100000"));
      assertThat(r1.get("d"), is(expectedLocalTime("2019-10-02 02:59:59")
          + ".456389"));
      assertThat(r1.get("e"), is("10:59:59.45638"));
      assertThat(r1.get("f"), is("2019-10-02 10:59:59"));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(ImmutableList.of(2)));
      assertThat(r2.get("a"), is(101));
      assertThat(r2.get("b"), is("1970-01-01 08:00:01.550"));
      assertThat(r2.get("c"), is("2022-01-01 00:01:03.123450"));
      assertThat(r2.get("d"), is(expectedLocalTime("1970-01-01 00:00:01")
          + ".000001"));
      assertThat(r2.get("e"), is("08:00:01.00000"));
      assertThat(r2.get("f"), is("1970-01-01 08:00:01"));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey(), is(ImmutableList.of(3)));
      assertThat(r3.get("a"), is(102));
      assertThat(r3.get("b"), is("2008-11-23 09:23:00.808"));
      assertThat(r3.get("c"), is("1999-12-31 00:01:03.123456"));
      assertThat(r3.get("d"), is(expectedLocalTime("2008-11-23 01:23:00")
          + ".294000"));
      assertThat(r3.get("e"), is("09:23:00.29400"));
      assertThat(r3.get("f"), is("2008-11-23 09:23:00"));
    };
  }
}
