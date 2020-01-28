package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnBooleanTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("boolean").setNullable(false))
        .addColumn(new Column().setName("b").setType("BOOL").setNullable(false));
  }

  @Test
  public void testBooleanColumnMysql56() {
    testBooleanColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/boolean/tb18.ibd");
  }

  @Test
  public void testBooleanColumnMysql57() {
    testBooleanColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/boolean/tb18.ibd");
  }

  @Test
  public void testBooleanColumnMysql80() {
    testBooleanColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/boolean/tb18.ibd");
  }

  public void testBooleanColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);

      assertThat(recordList.size(), is(2));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(1));
      assertThat(r1.get("a"), is(true));
      assertThat(r1.get("b"), is(false));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(2));
      assertThat(r2.get("a"), is(false));
      assertThat(r2.get("b"), is(true));
    }
  }
}
