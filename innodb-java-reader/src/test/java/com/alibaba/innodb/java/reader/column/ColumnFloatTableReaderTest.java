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
 * mysql> select * from tb15\G;
 * *************************** 1. row ***************************
 *       id: 1
 *  c_float: 0
 * c_double: 0
 * *************************** 2. row ***************************
 *       id: 2
 *  c_float: 1
 * c_double: -1
 * *************************** 3. row ***************************
 *       id: 3
 *  c_float: 222.22
 * c_double: 3333.333
 * *************************** 4. row ***************************
 *       id: 4
 *  c_float: 12345700
 * c_double: 1234567890.123456
 * *************************** 5. row ***************************
 *       id: 5
 *  c_float: -12345700
 * c_double: -1234567890.123456
 * </pre>
 *
 * @author xu.zx
 */
public class ColumnFloatTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11) unsigned").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("c_float").setType("float").setNullable(false))
        .addColumn(new Column().setName("c_double").setType("double").setNullable(false));
  }

  @Test
  public void testIntColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testIntColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testIntColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(5));

      GenericRecord r1 = recordList.get(0);
      System.out.println(Arrays.asList(r1.getValues()));
      assertThat(r1.getPrimaryKey(), is(ImmutableList.of(1L)));
      assertThat(r1.get("c_float"), is(0.0F));
      assertThat(r1.get("c_double"), is(0.0D));

      GenericRecord r2 = recordList.get(1);
      System.out.println(Arrays.asList(r2.getValues()));
      assertThat(r2.getPrimaryKey(), is(ImmutableList.of(2L)));
      assertThat(r2.get("c_float"), is(1.0F));
      assertThat(r2.get("c_double"), is(-1.0D));

      GenericRecord r3 = recordList.get(2);
      System.out.println(Arrays.asList(r3.getValues()));
      assertThat(r3.getPrimaryKey(), is(ImmutableList.of(3L)));
      assertThat(r3.get("c_float"), is(222.22F));
      assertThat(r3.get("c_double"), is(3333.333D));

      GenericRecord r4 = recordList.get(3);
      System.out.println(Arrays.asList(r4.getValues()));
      assertThat(r4.getPrimaryKey(), is(ImmutableList.of(4L)));
      assertThat(r4.get("c_float"), is(12345678.1234F));
      assertThat(r4.get("c_double"), is(1234567890.123456D));

      GenericRecord r5 = recordList.get(4);
      System.out.println(Arrays.asList(r5.getValues()));
      assertThat(r5.getPrimaryKey(), is(ImmutableList.of(5L)));
      assertThat(r5.get("c_float"), is(-12345678.1234F));
      assertThat(r5.get("c_double"), is(-1234567890.123456D));
    };
  }
}
