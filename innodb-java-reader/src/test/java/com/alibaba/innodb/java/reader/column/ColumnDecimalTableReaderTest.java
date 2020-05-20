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
        .addColumn(new Column().setName("d").setType("NUMERIC(6,3)").setNullable(false))
        .addColumn(new Column().setName("e").setType("DECIMAL").setNullable(false))
        .addColumn(new Column().setName("f").setType("decimal(30,25)").setNullable(true))
        .addColumn(new Column().setName("g").setType("decimal(38)").setNullable(true))
        .addColumn(new Column().setName("h").setType("decimal(38,30)").setNullable(true))
        .addColumn(new Column().setName("i").setType("decimal unsigned").setNullable(false));
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
      assertThat(recordList.size(), is(4));

      List<Object[]> expected = Arrays.asList(
          new Object[]{1, new BigDecimal("0"),
              new BigDecimal(".00000"),
              new BigDecimal("0"),
              new BigDecimal("0.000"),
              new BigDecimal("0"),
              new BigDecimal("0.0000000000000000000000000"),
              new BigDecimal("0"),
              new BigDecimal("0.000000000000000000000000000000"),
              new BigDecimal("0")
          },
          new Object[]{2, new BigDecimal("123456"),
              new BigDecimal("12345.67890"),
              new BigDecimal("12345678901"),
              new BigDecimal("123.100"),
              new BigDecimal("12346"),
              new BigDecimal("12345.1234567890123456789012345"),
              new BigDecimal("666"),
              new BigDecimal("0.123456789012345678901234567890"),
              new BigDecimal("76543")
          },
          new Object[]{3, new BigDecimal("-123456"),
              new BigDecimal("-1234.56789"),
              new BigDecimal("-12345678901"),
              new BigDecimal("3.142"),
              new BigDecimal("-12346"),
              null,
              new BigDecimal("12345678901234567890123456789012345678"),
              new BigDecimal("8.123456789012345678901234567890"),
              new BigDecimal("89")
          },
          new Object[]{4, new BigDecimal("9"),
              new BigDecimal("567.89100"),
              new BigDecimal("987654321"),
              new BigDecimal("456.000"),
              new BigDecimal("0"),
              new BigDecimal("0.0123456789012345678912345"),
              new BigDecimal("999"),
              null,
              new BigDecimal("0")
          }
      );

      for (int i = 0; i < recordList.size(); i++) {
        GenericRecord r = recordList.get(i);
        Object[] v = r.getValues();
        System.out.println(Arrays.asList(v));
        Object[] row = expected.get(i);
        assertThat(r.getPrimaryKey(), is(ImmutableList.of(row[0])));
        assertThat(r.get("a"), is(row[1]));
        assertThat(r.get("b"), is(row[2]));
        assertThat(r.get("c"), is(row[3]));
        assertThat(r.get("d"), is(row[4]));
        assertThat(r.get("e"), is(row[5]));
        assertThat(r.get("f"), is(row[6]));
        assertThat(r.get("g"), is(row[7]));
        assertThat(r.get("h"), is(row[8]));
      }
    };
  }
}
