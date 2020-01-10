package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnTextTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("TINYTEXT").setNullable(false))
        .addColumn(new Column().setName("b").setType("TEXT").setNullable(false))
        .addColumn(new Column().setName("c").setType("MEDIUMTEXT").setNullable(false))
        .addColumn(new Column().setName("d").setType("LONGTEXT").setNullable(false));
  }

  @Test
  public void testTextColumnMysql56() {
    testTextColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/text/tb08.ibd");
  }

  @Test
  public void testTextColumnMysql57() {
    testTextColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/text/tb08.ibd");
  }

  //FIXME New format of LOB page type not supported currently
  @Ignore
  @Test
  public void testTextColumnMysql80() {
    testTextColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/text/tb08.ibd");
  }

  public void testTextColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryAll();

      assertThat(recordList.size(), is(10));

      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        //System.out.println(Arrays.asList(values));

        assertThat(values[0], is(i));
        assertThat(record.get("a"), is(((char) (97 + i)) + StringUtils.repeat('a', 200)));

        assertThat(((String) record.get("b")).length(), is(60001));
        assertThat(record.get("b"), is(((char) (97 + i)) + StringUtils.repeat('b', 60000)));

        assertThat(((String) record.get("c")).length(), is(80001));
        assertThat(record.get("c"), is(((char) (97 + i)) + StringUtils.repeat('c', 80000)));

        assertThat(((String) record.get("d")).length(), is(100001));
        assertThat(record.get("d"), is(((char) (97 + i)) + StringUtils.repeat('d', 100000)));
      }
    }
  }
}
