package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * https://dev.mysql.com/doc/refman/5.6/en/integer-types.html
 *
 * <pre>
 * mysql> select * from tb02;
 * +-----+------------+-----------+-------------+------------+--------------+-------------+------------
 * +-------------+---------------------+----------------------+
 * | id  | c_utinyint | c_tinyint | c_usmallint | c_smallint | c_umediumint | c_mediumint | c_uint
 * | c_int       | c_ubigint           | c_bigint             |
 * +-----+------------+-----------+-------------+------------+--------------+-------------+------------
 * +-------------+---------------------+----------------------+
 * | 100 |        100 |       100 |       10000 |      10000 |      1000000 |     1000000 |   10000000
 * |    10000000 |        100000000000 |         100000000000 |
 * | 101 |        127 |       127 |       32767 |      32767 |      8388607 |     8388607 | 2147483647
 * |  2147483647 | 9223372036854775805 |  9223372036854775807 |
 * | 102 |        128 |      -128 |       32768 |     -32768 |      8388608 |    -8388608 | 2147483648
 * | -2147483648 | 9223372036854775806 | -9223372036854775807 |
 * | 103 |        129 |      -127 |       32769 |     -32767 |      8388609 |    -8388607 | 2147483649
 * | -2147483647 | 9223372036854775807 | -9223372036854775806 |
 * | 104 |        100 |       100 |       10000 |      10000 |      1000000 |    -1000000 |   10000000
 * |    10000000 |        100000000000 |         100000000000 |
 * +-----+------------+-----------+-------------+------------+--------------+-------------+------------
 * +-------------+---------------------+----------------------+
 * </pre>
 *
 * @author xu.zx
 */
public class ColumnIntTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11) unsigned").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("c_utinyint").setType("tinyint(11) unsigned").setNullable(false))
        .addColumn(new Column().setName("c_tinyint").setType("TINYINT(11) ").setNullable(false))
        .addColumn(new Column().setName("c_usmallint").setType("smallint(11) unsigned ").setNullable(false))
        .addColumn(new Column().setName("c_smallint").setType("SMallInt(11)").setNullable(false))
        .addColumn(new Column().setName("c_umediumint").setType("mediumint(11) unsigned").setNullable(false))
        .addColumn(new Column().setName("c_mediumint").setType(" MEDIUMINT(11)").setNullable(false))
        .addColumn(new Column().setName("c_uint").setType("int(11) UNSIGNED").setNullable(false))
        .addColumn(new Column().setName("c_int").setType("int(11) ").setNullable(false))
        .addColumn(new Column().setName("c_ubigint").setType("bigint(20) UNSIGNED").setNullable(false))
        .addColumn(new Column().setName("c_bigint").setType("bigint(20)").setNullable(false));
  }

  @Test
  public void testIntColumnMysql56() {
    testIntColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/int/tb02.ibd");
  }

  @Test
  public void testIntColumnMysql57() {
    testIntColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/int/tb02.ibd");
  }

  @Test
  public void testIntColumnMysql80() {
    testIntColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/int/tb02.ibd");
  }

  public void testIntColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);

      assertThat(recordList.size(), is(5));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(100L));
      assertThat(r1.get("c_utinyint"), is(100));
      assertThat(r1.get("c_tinyint"), is(100));
      assertThat(r1.get("c_usmallint"), is(10000));
      assertThat(r1.get("c_smallint"), is(10000));
      assertThat(r1.get("c_umediumint"), is(1000000));
      assertThat(r1.get("c_mediumint"), is(1000000));
      assertThat(r1.get("c_uint"), is(10000000L));
      assertThat(r1.get("c_int"), is(10000000));
      assertThat(r1.get("c_ubigint"), is(BigInteger.valueOf(100000000000L)));
      assertThat(r1.get("c_bigint"), is(100000000000L));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(101L));
      assertThat(r2.get("c_utinyint"), is(127));
      assertThat(r2.get("c_tinyint"), is(127));
      assertThat(r2.get("c_usmallint"), is(32767));
      assertThat(r2.get("c_smallint"), is(32767));
      assertThat(r2.get("c_umediumint"), is(8388607));
      assertThat(r2.get("c_mediumint"), is(8388607));
      assertThat(r2.get("c_uint"), is(2147483647L));
      assertThat(r2.get("c_int"), is(2147483647));
      assertThat(r2.get("c_ubigint"), is(BigInteger.valueOf(9223372036854775805L)));
      assertThat(r2.get("c_bigint"), is(9223372036854775807L));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey(), is(102L));
      assertThat(r3.get("c_utinyint"), is(128));
      assertThat(r3.get("c_tinyint"), is((int) Byte.MIN_VALUE));
      assertThat(r3.get("c_usmallint"), is(32768));
      assertThat(r3.get("c_smallint"), is((int) Short.MIN_VALUE));
      assertThat(r3.get("c_umediumint"), is(8388608));
      assertThat(r3.get("c_mediumint"), is(-8388608));
      assertThat(r3.get("c_uint"), is(2147483648L));
      assertThat(r3.get("c_int"), is(Integer.MIN_VALUE));
      assertThat(r3.get("c_ubigint"), is(BigInteger.valueOf(9223372036854775806L)));
      assertThat(r3.get("c_bigint"), is(Long.MIN_VALUE + 1));

      GenericRecord r4 = recordList.get(3);
      Object[] v4 = r4.getValues();
      System.out.println(Arrays.asList(v4));
      assertThat(r4.getPrimaryKey(), is(103L));
      assertThat(r4.get("c_utinyint"), is(129));
      assertThat(r4.get("c_tinyint"), is((int) Byte.MIN_VALUE + 1));
      assertThat(r4.get("c_usmallint"), is(32769));
      assertThat(r4.get("c_smallint"), is((int) Short.MIN_VALUE + 1));
      assertThat(r4.get("c_umediumint"), is(8388609));
      assertThat(r4.get("c_mediumint"), is(-8388607));
      assertThat(r4.get("c_uint"), is(2147483649L));
      assertThat(r4.get("c_int"), is(Integer.MIN_VALUE + 1));
      assertThat(r4.get("c_ubigint"), is(BigInteger.valueOf(9223372036854775807L)));
      assertThat(r4.get("c_bigint"), is(Long.MIN_VALUE + 2));

      GenericRecord r5 = recordList.get(4);
      Object[] v5 = r5.getValues();
      System.out.println(Arrays.asList(v5));
      assertThat(r5.get("c_mediumint"), is(-1000000));
    }
  }
}
