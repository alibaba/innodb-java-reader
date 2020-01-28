package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnCharsetForColumnTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("varchar(64)").setNullable(false).setCharset("gbk"))
        .setCharset("latin1");
  }

  @Test
  public void testTableCharsetForColumnMysql56() {
    testTableCharsetForColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/char/tb20.ibd");
  }

  public void testTableCharsetForColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);

      assertThat(recordList.size(), is(2));

      assertThat(recordList.get(0).get("id"), is(1));
      assertThat(recordList.get(0).get("a"), is("abc$&"));

      assertThat(recordList.get(1).get("id"), is(2));
      assertThat(recordList.get(1).get("a"), is("你好这里是哪里"));
    }
  }

}
