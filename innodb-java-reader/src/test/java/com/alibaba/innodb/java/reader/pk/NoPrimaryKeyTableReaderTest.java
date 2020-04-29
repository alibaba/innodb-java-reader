package com.alibaba.innodb.java.reader.pk;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * <pre>
 * +-----+-------+----------+
 * | a   | b     | c        |
 * +-----+-------+----------+
 * | 100 | Jason | xxxxxxxx |
 * | 200 | Eric  | yyyyyyy  |
 * | 300 | Tom   | zzzzzz   |
 * +-----+-------+----------+
 * </pre>
 *
 * @author xu.zx
 */
public class NoPrimaryKeyTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("a").setType("int(11)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(10)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(10)").setNullable(false));
  }

  @Test
  public void testNoPkMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testNoPkMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testNoPkMysql80() {
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
      assertThat(r1.getPrimaryKey().isEmpty(), is(true));
      assertThat(r1.get("a"), is(100));
      assertThat(r1.get("b"), is("Jason"));
      assertThat(r1.get("c"), is(StringUtils.repeat("x", 8)));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey().isEmpty(), is(true));
      assertThat(r2.get("a"), is(200));
      assertThat(r2.get("b"), is("Eric"));
      assertThat(r2.get("c"), is(StringUtils.repeat("y", 7)));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey().isEmpty(), is(true));
      assertThat(r3.get("a"), is(300));
      assertThat(r3.get("b"), is("Tom"));
      assertThat(r3.get("c"), is(StringUtils.repeat("z", 6)));
    };
  }
}
