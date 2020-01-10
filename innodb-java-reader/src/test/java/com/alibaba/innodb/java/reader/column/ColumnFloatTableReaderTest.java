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
public class ColumnFloatTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11) unsigned").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("c_float").setType("float").setNullable(false))
        .addColumn(new Column().setName("c_double").setType("double").setNullable(false));
  }

  @Test
  public void testIntColumnMysql56() {
    testIntColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/float/tb15.ibd");
  }

  @Test
  public void testIntColumnMysql57() {
    testIntColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/float/tb15.ibd");
  }

  @Test
  public void testIntColumnMysql80() {
    testIntColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/float/tb15.ibd");
  }

  public void testIntColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);

      assertThat(recordList.size(), is(5));

      GenericRecord r1 = recordList.get(0);
      System.out.println(Arrays.asList(r1.getValues()));
      assertThat(r1.getPrimaryKey(), is(1L));
      assertThat(r1.get("c_float"), is(0.0F));
      assertThat(r1.get("c_double"), is(0.0D));

      GenericRecord r2 = recordList.get(1);
      System.out.println(Arrays.asList(r2.getValues()));
      assertThat(r2.getPrimaryKey(), is(2L));
      assertThat(r2.get("c_float"), is(1.0F));
      assertThat(r2.get("c_double"), is(-1.0D));

      GenericRecord r3 = recordList.get(2);
      System.out.println(Arrays.asList(r3.getValues()));
      assertThat(r3.getPrimaryKey(), is(3L));
      assertThat(r3.get("c_float"), is(222.22F));
      assertThat(r3.get("c_double"), is(3333.333D));

      GenericRecord r4 = recordList.get(3);
      System.out.println(Arrays.asList(r4.getValues()));
      assertThat(r4.getPrimaryKey(), is(4L));
      assertThat(r4.get("c_float"), is(12345678.1234F));
      assertThat(r4.get("c_double"), is(1234567890.123456D));

      GenericRecord r5 = recordList.get(4);
      System.out.println(Arrays.asList(r5.getValues()));
      assertThat(r5.getPrimaryKey(), is(5L));
      assertThat(r5.get("c_float"), is(-12345678.1234F));
      assertThat(r5.get("c_double"), is(-1234567890.123456D));
    }
  }
}
