package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnTextTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("TINYTEXT").setNullable(false))
        .addColumn(new Column().setName("b").setType("TEXT").setNullable(false))
        .addColumn(new Column().setName("c").setType("MEDIUMTEXT").setNullable(false))
        .addColumn(new Column().setName("d").setType("LONGTEXT").setNullable(false));
  }

  @Test
  public void testTextColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testTextColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testTextColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {
      assertThat(recordList.size(), is(10));

      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        //System.out.println(Arrays.asList(values));

        assertThat(values[0], is(i));
        assertThat(record.get("a"), is(((char) (97 + i)) + StringUtils.repeat('a', 200)));

        // TODO mysql8.0 lob is not supported
        if (!isMysql8Flag.get()) {
          assertThat(((String) record.get("b")).length(), is(60001));
          assertThat(record.get("b"), is(((char) (97 + i)) + StringUtils.repeat('b', 60000)));

          assertThat(((String) record.get("c")).length(), is(80001));
          assertThat(record.get("c"), is(((char) (97 + i)) + StringUtils.repeat('c', 80000)));

          assertThat(((String) record.get("d")).length(), is(100001));
          assertThat(record.get("d"), is(((char) (97 + i)) + StringUtils.repeat('d', 100000)));
        }
      }
    };
  }
}
