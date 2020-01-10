package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnBlobTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("TINYBLOB").setNullable(false))
        .addColumn(new Column().setName("b").setType("BLOB").setNullable(false))
        .addColumn(new Column().setName("c").setType("MEDIUMBLOB").setNullable(false))
        .addColumn(new Column().setName("d").setType("LONGBLOB").setNullable(false));
  }

  @Test
  public void testBlobColumnMysql56() {
    testBlobColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/blob/tb09.ibd");
  }

  @Test
  public void testBlobColumnMysql57() {
    testBlobColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/blob/tb09.ibd");
  }

  //FIXME New format of LOB page type not supported currently
  @Ignore
  @Test
  public void testBlobColumnMysql80() {
    testBlobColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/blob/tb09.ibd");
  }

  public void testBlobColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryAll();

      assertThat(recordList.size(), is(10));

      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));

        assertThat(((byte[]) record.get("a")).length, is(201));
        assertThat(record.get("a"), is(getContent((byte) (97 + i), (byte) 0x0a, 200)));

        assertThat(((byte[]) record.get("b")).length, is(60001));
        assertThat(record.get("b"), is(getContent((byte) (97 + i), (byte) 0x0b, 60000)));

        assertThat(((byte[]) record.get("c")).length, is(80001));
        assertThat(record.get("c"), is(getContent((byte) (97 + i), (byte) 0x0c, 80000)));

        assertThat(((byte[]) record.get("d")).length, is(100001));
        assertThat(record.get("d"), is(getContent((byte) (97 + i), (byte) 0x0d, 100000)));
      }
    }
  }
}
