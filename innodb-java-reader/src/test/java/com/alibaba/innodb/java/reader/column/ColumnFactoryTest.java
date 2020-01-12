package com.alibaba.innodb.java.reader.column;

import org.junit.Test;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnFactoryTest {

  @Test
  public void testGetColumnJavaType() {
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.TINYINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_TINYINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.SMALLINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_SMALLINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.MEDIUMINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_MEDIUMINT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.INT).getName(), is(Integer.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.UNSIGNED_INT).getName(), is(Integer.class.getName()));
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
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.DATETIME).getName(), is(Date.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.TIMESTAMP).getName(), is(Timestamp.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.YEAR).getName(), is(Short.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.DATE).getName(), is(Date.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.FLOAT).getName(), is(Float.class.getName()));
    assertThat(ColumnFactory.getColumnJavaType(ColumnType.DOUBLE).getName(), is(Double.class.getName()));
  }

}
