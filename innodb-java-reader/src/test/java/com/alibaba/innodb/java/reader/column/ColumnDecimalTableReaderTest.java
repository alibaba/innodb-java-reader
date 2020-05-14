package com.alibaba.innodb.java.reader.column;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnDecimalTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("DECIMAL(6)").setNullable(false))
        .addColumn(new Column().setName("b").setType("DECIMAL(10,5)").setNullable(false))
        .addColumn(new Column().setName("c").setType("DECIMAL(12,0)").setNullable(false))
        .addColumn(new Column().setName("d").setType("NUMERIC(6,2)").setNullable(false))
        .addColumn(new Column().setName("e").setType("DECIMAL").setNullable(false));
  }

  @Test
  public void testDecimalColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testDecimalColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testDecimalColumnMysql80() {
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
      assertThat(r1.get("a"), is(new BigDecimal("0")));
      assertThat(r1.get("b"), is(new BigDecimal("0.00000")));
      assertThat(r1.get("c"), is(new BigDecimal("0")));
      assertThat(r1.get("d"), is(new BigDecimal("0.00")));
      assertThat(r1.get("e"), is(new BigDecimal("0")));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(ImmutableList.of(2)));
      assertThat(r2.get("a"), is(new BigDecimal("123456")));
      assertThat(r2.get("b"), is(new BigDecimal("12345.67890")));
      assertThat(r2.get("c"), is(new BigDecimal("12345678901")));
      assertThat(r2.get("d"), is(new BigDecimal("1234.10")));
      assertThat(r2.get("e"), is(new BigDecimal("12346")));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey(), is(ImmutableList.of(3)));
      assertThat(r3.get("a"), is(new BigDecimal("-123456")));
      assertThat(r3.get("b"), is(new BigDecimal("-12345.67890")));
      assertThat(r3.get("c"), is(new BigDecimal("-12345678901")));
      assertThat(r3.get("d"), is(new BigDecimal("3.14")));
      assertThat(r3.get("e"), is(new BigDecimal("-12346")));
    };
  }
}
