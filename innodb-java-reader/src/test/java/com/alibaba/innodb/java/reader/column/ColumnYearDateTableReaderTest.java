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

      assertThat(recordList.size(), is(8));

      List<Object[]> expected = Arrays.asList(
          new Object[]{1, (short) 0, "2100-11-11"},
          new Object[]{2, (short) 2001, "2155-01-01"},
          new Object[]{3, (short) 1901, "1900-01-01"},
          new Object[]{4, (short) 1999, "1901-12-31"},
          new Object[]{5, (short) 1969, "1969-10-02"},
          new Object[]{6, (short) 2020, "2020-12-31"},
          new Object[]{7, (short) 2100, "0069-01-10"},
          new Object[]{8, (short) 2155, "0001-01-01"}
      );

      for (int i = 0; i < recordList.size(); i++) {
        GenericRecord r = recordList.get(i);
        Object[] v = r.getValues();
        System.out.println(Arrays.asList(v));
        Object[] row = expected.get(i);
        assertThat(r.getPrimaryKey(), is(ImmutableList.of(row[0])));
        assertThat(r.get("a"), is(row[1]));
        assertThat(r.get("b"), is(row[2]));
      }
    };
  }
}
