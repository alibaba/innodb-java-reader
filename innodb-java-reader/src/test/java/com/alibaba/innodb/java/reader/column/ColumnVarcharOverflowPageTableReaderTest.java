package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnVarcharOverflowPageTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema().setCharset("utf8mb4") // table charset is utf8mb4
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(16380)").setNullable(false));
  }

  @Test
  public void testVarcharOverflowPageColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testVarcharOverflowPageColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testVarcharOverflowPageColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(50));

      int index = 0;
      for (int i = 1; i <= 50; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        // System.out.println(Arrays.asList(values)); // too long
        // TODO mysql8.0 lob is not supported
        if (!isMysql8Flag.get()) {
          assertThat(((String) record.get("b")).length(), is(8100));
          assertThat(record.get("b"), is(StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 8100)));
        } else {
          assertThat(((String) record.get("b")), is(StringUtils.repeat((char) 0x00, 8100)));
        }
      }
    };
  }
}
