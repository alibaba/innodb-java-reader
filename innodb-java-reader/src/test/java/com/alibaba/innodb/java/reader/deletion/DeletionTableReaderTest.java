package com.alibaba.innodb.java.reader.deletion;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test a complex table with insertion and deletion and re-insertion.
 *
 * @author xu.zx
 */
public class DeletionTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema().setCharset("utf8")
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(true));
  }

  @Test
  public void testDeletionTableQueryAllMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testDeletionTableQueryAllMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testDeletionTableQueryAllMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {
      assertThat(recordList.size(), is(2000));

      int pk = 1;
      for (int i = 0; i < 1000; i++) {
        GenericRecord r = recordList.get(i);
        Object[] v = r.getValues();
        assertThat(v[0], is(pk));
        assertThat(v[1], is(pk * 2L));
        assertThat(v[2], is(StringUtils.repeat("A", 16)));
        assertThat(v[3], is(StringUtils.repeat("C", 8) + ((char) (97 + pk % 26))));
        pk += 2;
      }

      pk = 2001;
      for (int i = 0; i < 1000; i++) {
        GenericRecord r = recordList.get(i + 1000);
        Object[] v = r.getValues();
        assertThat(v[0], is(pk));
        assertThat(v[1], is(pk * 5L));
        assertThat(v[2], is(StringUtils.repeat("我", 8)));
        assertThat(v[3], is(StringUtils.repeat("你", 4) + ((char) (97 + pk % 26))));
        pk++;
      }
    };
  }

}
