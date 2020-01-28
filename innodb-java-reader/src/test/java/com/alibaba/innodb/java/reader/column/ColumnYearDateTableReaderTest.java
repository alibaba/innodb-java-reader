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
 * <pre>
 * insert into tb16 values(null, 1900, '1900-01-01');
 * insert into tb16 values(null, 1901, '1901-12-31');
 * insert into tb16 values(null, 1969, '1969-10-02');
 * insert into tb16 values(null, 2020, '2020-01-29');
 * </pre>
 *
 * @author xu.zx
 */
public class ColumnYearDateTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("year").setNullable(false))
        .addColumn(new Column().setName("b").setType("date").setNullable(false));
  }

  @Test
  public void testYearDateColumnMysql56() {
    testYearDateColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/time/tb16.ibd");
  }

  @Test
  public void testYearDateColumnMysql57() {
    testYearDateColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/time/tb16.ibd");
  }

  @Test
  public void testYearDateColumnMysql80() {
    testYearDateColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/time/tb16.ibd");
  }

  public void testYearDateColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);

      assertThat(recordList.size(), is(4));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(1));
      assertThat(r1.get("a"), is((short) 1901));
      assertThat(r1.get("b"), is("1900-01-01"));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(2));
      assertThat(r2.get("a"), is((short) 1901));
      assertThat(r2.get("b"), is("1901-12-31"));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey(), is(3));
      assertThat(r3.get("a"), is((short) 1969));
      assertThat(r3.get("b"), is("1969-10-02"));

      GenericRecord r4 = recordList.get(3);
      Object[] v4 = r4.getValues();
      System.out.println(Arrays.asList(v4));
      assertThat(r4.getPrimaryKey(), is(4));
      assertThat(r4.get("a"), is((short) 2020));
      assertThat(r4.get("b"), is("2020-01-29"));
    }
  }
}
