package com.alibaba.innodb.java.reader.nullcolumn;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class NullColumnTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(true))
        .addColumn(new Column().setName("b").setType("varchar(32)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(32)").setNullable(true))
        .addColumn(new Column().setName("d").setType("varchar(32)").setNullable(true))
        .addColumn(new Column().setName("e").setType("text").setNullable(false))
        .addColumn(new Column().setName("f").setType("varchar(32)").setNullable(true));
  }

  @Test
  public void testNullColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testNullColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testNullColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      GenericRecord r1 = recordList.get(0);
      System.out.println(Arrays.asList(r1.getValues()));
      assertThat(r1.get("a"), is(1L));
      assertThat(r1.get("b"), is(StringUtils.repeat("a1", 16)));
      assertThat(r1.get("c"), is(StringUtils.repeat("a1", 16)));
      assertThat(r1.get("d"), is(StringUtils.repeat("a1", 16)));
      assertThat(r1.get("e"), is(StringUtils.repeat("a1", 16)));
      assertThat(r1.get("f"), is(StringUtils.repeat("a1", 16)));

      GenericRecord r2 = recordList.get(1);
      System.out.println(Arrays.asList(r2.getValues()));
      assertThat(r2.get("a"), is(999L));
      assertThat(r2.get("b"), is(StringUtils.repeat("a2", 16)));
      assertThat(r2.get("c"), is(StringUtils.repeat("a2", 16)));
      assertThat(r2.get("d"), is(StringUtils.repeat("a2", 16)));
      assertThat(r2.get("e"), is(StringUtils.repeat("a2", 16)));
      assertThat(r2.get("f"), nullValue());

      GenericRecord r3 = recordList.get(2);
      System.out.println(Arrays.asList(r3.getValues()));
      assertThat(r3.get("a"), is(2L));
      assertThat(r3.get("b"), is(StringUtils.repeat("a3", 16)));
      assertThat(r3.get("c"), nullValue());
      assertThat(r3.get("d"), is(StringUtils.repeat("a3", 16)));
      assertThat(r3.get("e"), is(StringUtils.repeat("a3", 16)));
      assertThat(r3.get("f"), nullValue());

      GenericRecord r4 = recordList.get(3);
      System.out.println(Arrays.asList(r3.getValues()));
      assertThat(r4.get("a"), is(3L));
      assertThat(r4.get("b"), is(StringUtils.repeat("a4", 16)));
      assertThat(r4.get("c"), nullValue());
      assertThat(r4.get("d"), is(StringUtils.repeat("a4", 16)));
      assertThat(r4.get("e"), is(StringUtils.repeat("a4", 16)));
      assertThat(r4.get("f"), is(StringUtils.repeat("a4", 16)));
    };
  }

}
