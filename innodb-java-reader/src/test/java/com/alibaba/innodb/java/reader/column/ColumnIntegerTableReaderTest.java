package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * https://dev.mysql.com/doc/refman/5.6/en/integer-types.html
 *
 * <pre>
 * mysql> select * from tb02\G;
 * *************************** 1. row ***************************
 *           id: 100
 *   c_utinyint: 0
 *    c_tinyint: 0
 *  c_usmallint: 0
 *   c_smallint: 0
 * c_umediumint: 0
 *  c_mediumint: 0
 *       c_uint: 0
 *        c_int: 0
 *    c_ubigint: 0
 *     c_bigint: 0
 * *************************** 2. row ***************************
 *           id: 101
 *   c_utinyint: 1
 *    c_tinyint: -1
 *  c_usmallint: 1
 *   c_smallint: -1
 * c_umediumint: 1
 *  c_mediumint: -1
 *       c_uint: 1
 *        c_int: -1
 *    c_ubigint: 1
 *     c_bigint: -1
 * *************************** 3. row ***************************
 *           id: 102
 *   c_utinyint: 1
 *    c_tinyint: 1
 *  c_usmallint: 1
 *   c_smallint: 1
 * c_umediumint: 1
 *  c_mediumint: 1
 *       c_uint: 1
 *        c_int: 1
 *    c_ubigint: 1
 *     c_bigint: 1
 * *************************** 4. row ***************************
 *           id: 103
 *   c_utinyint: 100
 *    c_tinyint: 100
 *  c_usmallint: 10000
 *   c_smallint: 10000
 * c_umediumint: 1000000
 *  c_mediumint: 1000000
 *       c_uint: 10000000
 *        c_int: 10000000
 *    c_ubigint: 100000000000
 *     c_bigint: 100000000000
 * *************************** 5. row ***************************
 *           id: 104
 *   c_utinyint: 100
 *    c_tinyint: -100
 *  c_usmallint: 10000
 *   c_smallint: -10000
 * c_umediumint: 1000000
 *  c_mediumint: -1000000
 *       c_uint: 10000000
 *        c_int: -10000000
 *    c_ubigint: 100000000000
 *     c_bigint: -100000000000
 * *************************** 6. row ***************************
 *           id: 105
 *   c_utinyint: 126
 *    c_tinyint: 126
 *  c_usmallint: 32766
 *   c_smallint: 32766
 * c_umediumint: 8388606
 *  c_mediumint: 8388606
 *       c_uint: 2147483646
 *        c_int: 2147483646
 *    c_ubigint: 9223372036854775806
 *     c_bigint: 9223372036854775806
 * *************************** 7. row ***************************
 *           id: 106
 *   c_utinyint: 127
 *    c_tinyint: 127
 *  c_usmallint: 32767
 *   c_smallint: 32767
 * c_umediumint: 8388607
 *  c_mediumint: 8388607
 *       c_uint: 2147483647
 *        c_int: 2147483647
 *    c_ubigint: 9223372036854775807
 *     c_bigint: 9223372036854775807
 * *************************** 8. row ***************************
 *           id: 107
 *   c_utinyint: 128
 *    c_tinyint: -128
 *  c_usmallint: 32768
 *   c_smallint: -32768
 * c_umediumint: 8388608
 *  c_mediumint: -8388608
 *       c_uint: 2147483648
 *        c_int: -2147483648
 *    c_ubigint: 9223372036854775808
 *     c_bigint: -9223372036854775808
 * *************************** 9. row ***************************
 *           id: 108
 *   c_utinyint: 129
 *    c_tinyint: -127
 *  c_usmallint: 32769
 *   c_smallint: -32767
 * c_umediumint: 8388609
 *  c_mediumint: -8388607
 *       c_uint: 2147483649
 *        c_int: -2147483647
 *    c_ubigint: 9223372036854775809
 *     c_bigint: -9223372036854775807
 * </pre>
 *
 * @author xu.zx
 */
public class ColumnIntegerTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
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
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testIntColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testIntColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {
      assertThat(recordList.size(), is(9));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(100L));
      assertThat(r1.get("c_utinyint"), is(0));
      assertThat(r1.get("c_tinyint"), is(0));
      assertThat(r1.get("c_usmallint"), is(0));
      assertThat(r1.get("c_smallint"), is(0));
      assertThat(r1.get("c_umediumint"), is(0));
      assertThat(r1.get("c_mediumint"), is(0));
      assertThat(r1.get("c_uint"), is(0L));
      assertThat(r1.get("c_int"), is(0));
      assertThat(r1.get("c_ubigint"), is(BigInteger.valueOf(0L)));
      assertThat(r1.get("c_bigint"), is(0L));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(101L));
      assertThat(r2.get("c_utinyint"), is(1));
      assertThat(r2.get("c_tinyint"), is(-1));
      assertThat(r2.get("c_usmallint"), is(1));
      assertThat(r2.get("c_smallint"), is(-1));
      assertThat(r2.get("c_umediumint"), is(1));
      assertThat(r2.get("c_mediumint"), is(-1));
      assertThat(r2.get("c_uint"), is(1L));
      assertThat(r2.get("c_int"), is(-1));
      assertThat(r2.get("c_ubigint"), is(BigInteger.valueOf(1L)));
      assertThat(r2.get("c_bigint"), is(-1L));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey(), is(102L));
      assertThat(r3.get("c_utinyint"), is(1));
      assertThat(r3.get("c_tinyint"), is(1));
      assertThat(r3.get("c_usmallint"), is(1));
      assertThat(r3.get("c_smallint"), is(1));
      assertThat(r3.get("c_umediumint"), is(1));
      assertThat(r3.get("c_mediumint"), is(1));
      assertThat(r3.get("c_uint"), is(1L));
      assertThat(r3.get("c_int"), is(1));
      assertThat(r3.get("c_ubigint"), is(BigInteger.valueOf(1L)));
      assertThat(r3.get("c_bigint"), is(1L));

      GenericRecord r4 = recordList.get(3);
      Object[] v4 = r4.getValues();
      System.out.println(Arrays.asList(v4));
      assertThat(r4.getPrimaryKey(), is(103L));
      assertThat(r4.get("c_utinyint"), is(100));
      assertThat(r4.get("c_tinyint"), is(100));
      assertThat(r4.get("c_usmallint"), is(10000));
      assertThat(r4.get("c_smallint"), is(10000));
      assertThat(r4.get("c_umediumint"), is(1000000));
      assertThat(r4.get("c_mediumint"), is(1000000));
      assertThat(r4.get("c_uint"), is(10000000L));
      assertThat(r4.get("c_int"), is(10000000));
      assertThat(r4.get("c_ubigint"), is(BigInteger.valueOf(100000000000L)));
      assertThat(r4.get("c_bigint"), is(100000000000L));

      GenericRecord r5 = recordList.get(4);
      Object[] v5 = r5.getValues();
      System.out.println(Arrays.asList(v5));
      assertThat(r5.getPrimaryKey(), is(104L));
      assertThat(r5.get("c_utinyint"), is(100));
      assertThat(r5.get("c_tinyint"), is(-100));
      assertThat(r5.get("c_usmallint"), is(10000));
      assertThat(r5.get("c_smallint"), is(-10000));
      assertThat(r5.get("c_umediumint"), is(1000000));
      assertThat(r5.get("c_mediumint"), is(-1000000));
      assertThat(r5.get("c_uint"), is(10000000L));
      assertThat(r5.get("c_int"), is(-10000000));
      assertThat(r5.get("c_ubigint"), is(BigInteger.valueOf(100000000000L)));
      assertThat(r5.get("c_bigint"), is(-100000000000L));

      GenericRecord r6 = recordList.get(5);
      Object[] v6 = r6.getValues();
      System.out.println(Arrays.asList(v6));
      assertThat(r6.getPrimaryKey(), is(105L));
      assertThat(r6.get("c_utinyint"), is(Byte.MAX_VALUE - 1));
      assertThat(r6.get("c_tinyint"), is(Byte.MAX_VALUE - 1));
      assertThat(r6.get("c_usmallint"), is(Short.MAX_VALUE - 1));
      assertThat(r6.get("c_smallint"), is(Short.MAX_VALUE - 1));
      assertThat(r6.get("c_umediumint"), is(8388607 - 1));
      assertThat(r6.get("c_mediumint"), is(8388607 - 1));
      assertThat(r6.get("c_uint"), is((long) Integer.MAX_VALUE - 1));
      assertThat(r6.get("c_int"), is(Integer.MAX_VALUE - 1));
      assertThat(r6.get("c_ubigint"), is(BigInteger.valueOf(Long.MAX_VALUE - 1)));
      assertThat(r6.get("c_bigint"), is(Long.MAX_VALUE - 1));

      GenericRecord r7 = recordList.get(6);
      Object[] v7 = r7.getValues();
      System.out.println(Arrays.asList(v7));
      assertThat(r7.getPrimaryKey(), is(106L));
      assertThat(r7.get("c_utinyint"), is((int) Byte.MAX_VALUE));
      assertThat(r7.get("c_tinyint"), is((int) Byte.MAX_VALUE));
      assertThat(r7.get("c_usmallint"), is((int) Short.MAX_VALUE));
      assertThat(r7.get("c_smallint"), is((int) Short.MAX_VALUE));
      assertThat(r7.get("c_umediumint"), is(8388607));
      assertThat(r7.get("c_mediumint"), is(8388607));
      assertThat(r7.get("c_uint"), is((long) Integer.MAX_VALUE));
      assertThat(r7.get("c_int"), is(Integer.MAX_VALUE));
      assertThat(r7.get("c_ubigint"), is(BigInteger.valueOf(Long.MAX_VALUE)));
      assertThat(r7.get("c_bigint"), is(Long.MAX_VALUE));

      GenericRecord r8 = recordList.get(7);
      Object[] v8 = r8.getValues();
      System.out.println(Arrays.asList(v7));
      assertThat(r8.getPrimaryKey(), is(107L));
      assertThat(r8.get("c_utinyint"), is(Byte.MAX_VALUE + 1));
      assertThat(r8.get("c_tinyint"), is((int) Byte.MIN_VALUE));
      assertThat(r8.get("c_usmallint"), is(Short.MAX_VALUE + 1));
      assertThat(r8.get("c_smallint"), is((int) Short.MIN_VALUE));
      assertThat(r8.get("c_umediumint"), is(8388608));
      assertThat(r8.get("c_mediumint"), is(-8388608));
      assertThat(r8.get("c_uint"), is(Integer.MAX_VALUE + 1L));
      assertThat(r8.get("c_int"), is(Integer.MIN_VALUE));
      assertThat(r8.get("c_ubigint"), is(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)));
      assertThat(r8.get("c_bigint"), is(Long.MIN_VALUE));

      GenericRecord r9 = recordList.get(8);
      Object[] v9 = r9.getValues();
      System.out.println(Arrays.asList(v9));
      assertThat(r9.getPrimaryKey(), is(108L));
      assertThat(r9.get("c_utinyint"), is(Byte.MAX_VALUE + 2));
      assertThat(r9.get("c_tinyint"), is((int) Byte.MIN_VALUE + 1));
      assertThat(r9.get("c_usmallint"), is(Short.MAX_VALUE + 2));
      assertThat(r9.get("c_smallint"), is((int) Short.MIN_VALUE + 1));
      assertThat(r9.get("c_umediumint"), is(8388609));
      assertThat(r9.get("c_mediumint"), is(-8388607));
      assertThat(r9.get("c_uint"), is(Integer.MAX_VALUE + 2L));
      assertThat(r9.get("c_int"), is(Integer.MIN_VALUE + 1));
      assertThat(r9.get("c_ubigint"), is(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(2L))));
      assertThat(r9.get("c_bigint"), is(Long.MIN_VALUE + 1));
    };
  }
}
