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

/**
 * <pre>
 * insert into tb16 values(null, 1900, '1900-01-01');
 * insert into tb16 values(null, 1901, '1901-12-31');
 * insert into tb16 values(null, 1969, '1969-10-02');
 * insert into tb16 values(null, 2020, '2020-01-29');
 * </pre>
 *
 * @author xu.zx
 */
public class ColumnYearDateTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("year").setNullable(false))
        .addColumn(new Column().setName("b").setType("date").setNullable(false));
  }

  @Test
  public void testYearDateColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testYearDateColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testYearDateColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(4));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(ImmutableList.of(1)));
      assertThat(r1.get("a"), is((short) 1901));
      assertThat(r1.get("b"), is("1900-01-01"));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(ImmutableList.of(2)));
      assertThat(r2.get("a"), is((short) 1901));
      assertThat(r2.get("b"), is("1901-12-31"));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey(), is(ImmutableList.of(3)));
      assertThat(r3.get("a"), is((short) 1969));
      assertThat(r3.get("b"), is("1969-10-02"));

      GenericRecord r4 = recordList.get(3);
      Object[] v4 = r4.getValues();
      System.out.println(Arrays.asList(v4));
      assertThat(r4.getPrimaryKey(), is(ImmutableList.of(4)));
      assertThat(r4.get("a"), is((short) 2020));
      assertThat(r4.get("b"), is("2020-01-29"));
    };
  }
}
