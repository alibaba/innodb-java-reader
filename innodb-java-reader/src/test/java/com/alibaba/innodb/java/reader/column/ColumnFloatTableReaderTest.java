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
 * @author xu.zx
 */
public class ColumnFloatTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11) unsigned").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("c_float").setType("float").setNullable(false))
        .addColumn(new Column().setName("c_float2").setType("FLOAT(7,4)").setNullable(false))
        .addColumn(new Column().setName("c_real").setType("real").setNullable(false))
        .addColumn(new Column().setName("c_double").setType("double").setNullable(false))
        .addColumn(new Column().setName("c_double2").setType("DOUBLE(15,5)").setNullable(false))
        .addColumn(new Column().setName("c_double3").setType("double UNSIGNED").setNullable(false));
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

      assertThat(recordList.size(), is(6));

      List<Object[]> expected = Arrays.asList(
          new Object[]{1L, 0f, 0.0000f, 0f, 0d, 0.0000d, 0d},
          new Object[]{2L, 0.56789f, 999.0001f, 0.12345f, 0.987654321d, 1234567890.12345d, 1d},
          new Object[]{3L, 1f, 0.0000f, -1f, -1d, -1234567890.12345d, 2d},
          new Object[]{4L, 222.22f, 3.1400f, 222.22f, 3333.333d, 1234.56789d, 3d},
          new Object[]{5L, 1.2345678E7F, 256.7890f, 1.2345678E7F, 1234567890.123456d, -56.78900d, 4d},
          new Object[]{6L, -1.2345678E7F, 333.2222f, -1.2345678E7F, -1234567890.123456d, -0.87654d, 5d});

      for (int i = 0; i < recordList.size(); i++) {
        GenericRecord r = recordList.get(i);
        Object[] v = r.getValues();
        System.out.println(Arrays.asList(v));
        Object[] row = expected.get(i);
        assertThat(r.getPrimaryKey(), is(ImmutableList.of(row[0])));
        assertThat(r.get("c_float"), is(row[1]));
        assertThat(r.get("c_float2"), is(row[2]));
        assertThat(r.get("c_real"), is(row[3]));
        assertThat(r.get("c_double"), is(row[4]));
        assertThat(r.get("c_double2"), is(row[5]));
        assertThat(r.get("c_double3"), is(row[6]));
      }

      GenericRecord r1 = recordList.get(0);
      System.out.println(Arrays.asList(r1.getValues()));
      assertThat(r1.getPrimaryKey(), is(ImmutableList.of(1L)));
      assertThat(r1.get("c_float"), is(0.0F));
      assertThat(r1.get("c_real"), is(0.0F));
      assertThat(r1.get("c_double"), is(0.0D));

      GenericRecord r2 = recordList.get(1);
      System.out.println(Arrays.asList(r2.getValues()));
      assertThat(r2.getPrimaryKey(), is(ImmutableList.of(2L)));
      assertThat(r2.get("c_float"), is(0.56789F));
      assertThat(r2.get("c_real"), is(0.12345F));
      assertThat(r2.get("c_double"), is(0.987654321D));

      GenericRecord r3 = recordList.get(2);
      System.out.println(Arrays.asList(r2.getValues()));
      assertThat(r3.getPrimaryKey(), is(ImmutableList.of(3L)));
      assertThat(r3.get("c_float"), is(1.0F));
      assertThat(r3.get("c_real"), is(-1.0F));
      assertThat(r3.get("c_double"), is(-1.0D));

      GenericRecord r4 = recordList.get(3);
      System.out.println(Arrays.asList(r3.getValues()));
      assertThat(r4.getPrimaryKey(), is(ImmutableList.of(4L)));
      assertThat(r4.get("c_float"), is(222.22F));
      assertThat(r4.get("c_real"), is(222.22F));
      assertThat(r4.get("c_double"), is(3333.333D));

      GenericRecord r5 = recordList.get(4);
      System.out.println(Arrays.asList(r4.getValues()));
      assertThat(r5.getPrimaryKey(), is(ImmutableList.of(5L)));
      assertThat(r5.get("c_float"), is(12345678.1234F));
      assertThat(r5.get("c_real"), is(12345678.1234F));
      assertThat(r5.get("c_double"), is(1234567890.123456D));

      GenericRecord r6 = recordList.get(5);
      System.out.println(Arrays.asList(r5.getValues()));
      assertThat(r6.getPrimaryKey(), is(ImmutableList.of(6L)));
      assertThat(r6.get("c_float"), is(-12345678.1234F));
      assertThat(r6.get("c_real"), is(-12345678.1234F));
      assertThat(r6.get("c_double"), is(-1234567890.123456D));
    };
  }
}
