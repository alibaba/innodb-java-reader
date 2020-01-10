package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
@Ignore
public class ColumnVarcharOverflowPageTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(1024)").setNullable(false));
  }

  @Test
  public void testVarcharOverflowPageColumnMysql56() {
    testVarcharOverflowPageColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/char/tb06.ibd");
  }

  @Test
  public void testVarcharOverflowPageColumnMysql57() {
    testVarcharOverflowPageColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/char/tb06.ibd");
  }

  //FIXME New format of LOB page type not supported currently
  @Test(expected = IllegalStateException.class)
  public void testVarcharOverflowPageColumnMysql80() {
    testVarcharOverflowPageColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/char/tb06.ibd");
  }

  public void testVarcharOverflowPageColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryAll();

      assertThat(recordList.size(), is(50));

      int index = 0;
      for (int i = 1; i <= 50; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));

        assertThat(((String) record.get("b")).length(), is(8100));
        assertThat(record.get("b"), is(StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 8100)));
      }
    }
  }
}
