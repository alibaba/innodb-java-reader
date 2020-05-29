package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.exception.ColumnParseException;
import com.alibaba.innodb.java.reader.util.BitLiteral;
import com.alibaba.innodb.java.reader.util.MultiEnumLiteral;
import com.alibaba.innodb.java.reader.util.SingleEnumLiteral;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnFactoryTest {

  @Test
  public void testGetColumnJavaType() {
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.TINYINT).getName(), is(Byte.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_TINYINT).getName(), is(Short.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.SMALLINT).getName(), is(Short.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_SMALLINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.MEDIUMINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_MEDIUMINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.INT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_INT).getName(), is(Long.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.BIGINT).getName(), is(Long.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_BIGINT).getName(), is(BigInteger.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.CHAR).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.VARCHAR).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.BINARY).getName(), is(byte[].class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.VARBINARY).getName(), is(byte[].class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.TINYTEXT).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.TEXT).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.MEDIUMTEXT).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.LONGTEXT).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.TINYBLOB).getName(), is(byte[].class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.BLOB).getName(), is(byte[].class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.MEDIUMBLOB).getName(), is(byte[].class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.LONGBLOB).getName(), is(byte[].class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.DATETIME).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.TIMESTAMP).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.TIME).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.YEAR).getName(), is(Short.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.DATE).getName(), is(String.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.FLOAT).getName(), is(Float.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.REAL).getName(), is(Float.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.DOUBLE).getName(), is(Double.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.DECIMAL).getName(), is(BigDecimal.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.NUMERIC).getName(), is(BigDecimal.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.BOOL).getName(), is(Boolean.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.BOOLEAN).getName(), is(Boolean.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.ENUM).getName(), is(SingleEnumLiteral.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.SET).getName(), is(MultiEnumLiteral.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.BIT).getName(), is(BitLiteral.class.getName()));
  }

  @Test
  public void testGetColumnJavaTypeFunc() {
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.TINYINT).apply("100"),
        is((byte) 100));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.UNSIGNED_TINYINT).apply("100"),
        is((short) 100));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.SMALLINT).apply("100"),
        is((short) 100));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.UNSIGNED_SMALLINT).apply("100"),
        is(100));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.MEDIUMINT).apply("100"),
        is(100));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.UNSIGNED_MEDIUMINT).apply("100"),
        is(100));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.INT).apply("100"),
        is(100));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.UNSIGNED_INT).apply("100"),
        is(100L));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.BIGINT).apply("100"),
        is(100L));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.UNSIGNED_BIGINT).apply("100"),
        is(BigInteger.valueOf(100L)));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.CHAR).apply("hello"),
        is("hello"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.VARCHAR).apply("hello"),
        is("hello"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.BINARY).apply("hello"),
        is("hello"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.VARBINARY).apply("hello"),
        is("hello"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.TINYTEXT).apply("hello"),
        is("hello"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.TEXT).apply("hello"),
        is("hello"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.MEDIUMTEXT).apply("hello"),
        is("hello"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.LONGTEXT).apply("hello"),
        is("hello"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.TINYBLOB).apply("0x00"),
        is("0x00"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.BLOB).apply("0x00"),
        is("0x00"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.MEDIUMBLOB).apply("0x00"),
        is("0x00"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.LONGBLOB).apply("0x00"),
        is("0x00"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.DATETIME).apply("2010-01-01 00:00:11"),
        is("2010-01-01 00:00:11"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.TIMESTAMP).apply("2010-01-01 00:00:11"),
        is("2010-01-01 00:00:11"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.TIME).apply("00:00:00"),
        is("00:00:00"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.YEAR).apply("0000"),
        is((short) 0));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.YEAR).apply("2012"),
        is((short) 2012));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.DATE).apply("2012-01-11"),
        is("2012-01-11"));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.FLOAT).apply("7.18"),
        is(7.18F));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.REAL).apply("1.2345678"),
        is(1.2345678F));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.DOUBLE).apply("123456.123456789"),
        is(123456.1234567890D));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.DECIMAL).apply("123456.123456789"),
        is(BigDecimal.valueOf(123456.123456789D)));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.NUMERIC).apply("123456.123456789"),
        is(BigDecimal.valueOf(123456.123456789D)));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.BOOL).apply("true"),
        is(true));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.BOOLEAN).apply("false"),
        is(false));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.ENUM).apply("mysql"),
        is(new SingleEnumLiteral(0, "mysql")));
    assertThat(ColumnFactory.getColumnToJavaTypeFunc(ColumnType.SET).apply("hello,world"),
        is(new MultiEnumLiteral(2).add(0, "hello").add(0, "world")));
  }

  /**
   * Does not support Spatial Index.<p>
   * For example,
   * <code>CREATE TABLE geom (g GEOMETRY NOT NULL SRID 4326, SPATIAL INDEX(g));</code>
   */
  @Test(expected = ColumnParseException.class)
  public void testGetColumnJavaTypeNegate() {
    ColumnFactory.getColumnJavaType("GEOMETRY").getName();
  }

}
