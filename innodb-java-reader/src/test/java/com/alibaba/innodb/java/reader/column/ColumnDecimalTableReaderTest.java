package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * <pre>
 * +----+--------+-------------+-------------+-------+
 * | id | a      | b           | c           | d     |
 * +----+--------+-------------+-------------+-------+
 * |  1 | 123456 | 12345.67890 | 12345678901 | 12346 |
 * +----+--------+-------------+-------------+-------+
 * </pre>
 *
 * @author xu.zx
 */
public class ColumnDecimalTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("DECIMAL(6)").setNullable(false))
        .addColumn(new Column().setName("b").setType("DECIMAL(10,5)").setNullable(false))
        .addColumn(new Column().setName("c").setType("DECIMAL(12,0)").setNullable(false))
        .addColumn(new Column().setName("d").setType("DECIMAL").setNullable(false));
  }

  @Test
  public void testDecimalColumnMysql56() {
    testDecimalColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/decimal/tb19.ibd");
  }

  @Test
  public void testDecimalColumnMysql57() {
    testDecimalColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/decimal/tb19.ibd");
  }

  @Test
  public void testDecimalColumnMysql80() {
    testDecimalColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/decimal/tb19.ibd");
  }

  public void testDecimalColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);

      assertThat(recordList.size(), is(1));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(1));
      assertThat(r1.get("a"), is(new BigDecimal("123456")));
      assertThat(r1.get("b"), is(new BigDecimal("12345.67890")));
      assertThat(r1.get("c"), is(new BigDecimal("12345678901")));
      assertThat(r1.get("d"), is(new BigDecimal("12346")));
    }
  }
}
