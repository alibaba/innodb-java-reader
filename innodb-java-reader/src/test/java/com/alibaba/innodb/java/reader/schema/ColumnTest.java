package com.alibaba.innodb.java.reader.schema;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnTest {

  @Test
  public void testIntColumn() {
    Column column = new Column().setName("c1").setType("INT(10)");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getOrdinal(), is(0));
    assertThat(column.getLength(), is(10));
    assertThat(column.getMaxBytesPerChar(), is(1));
    assertThat(column.getPrecision(), is(0));
    assertThat(column.getScale(), is(0));
    assertThat(column.getType(), is("INT"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), nullValue());
    assertThat(column.getJavaCharset(), nullValue());
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
  }

  @Test
  public void testBigintUnsignedColumn() {
    Column column = new Column().setName("c1").setType("BIGINT(20) UNSIGNED")
        .setPrimaryKey(true).setNullable(true);
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getOrdinal(), is(0));
    assertThat(column.getLength(), is(20));
    assertThat(column.getMaxBytesPerChar(), is(1));
    assertThat(column.getPrecision(), is(0));
    assertThat(column.getScale(), is(0));
    assertThat(column.getType(), is("BIGINT UNSIGNED"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), nullValue());
    assertThat(column.getJavaCharset(), nullValue());
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(true));
    assertThat(column.isPrimaryKey(), is(true));
  }

  @Test
  public void testToUpperCaseColumn() {
    Column column = new Column().setName("c1").setType("biGInt");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getOrdinal(), is(0));
    assertThat(column.getLength(), is(0));
    assertThat(column.getMaxBytesPerChar(), is(1));
    assertThat(column.getPrecision(), is(0));
    assertThat(column.getScale(), is(0));
    assertThat(column.getFullType(), is("biGInt"));
    assertThat(column.getType(), is("BIGINT"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), nullValue());
    assertThat(column.getJavaCharset(), nullValue());
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
  }

  @Test
  public void testCharsetColumn() {
    Column column = new Column().setName("c1").setType("VARCHAR(32)")
        .setCharset("utf8mb4");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getOrdinal(), is(0));
    assertThat(column.getLength(), is(32));
    assertThat(column.getMaxBytesPerChar(), is(4));
    assertThat(column.getPrecision(), is(0));
    assertThat(column.getScale(), is(0));
    assertThat(column.getFullType(), is("VARCHAR(32)"));
    assertThat(column.getType(), is("VARCHAR"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), is("utf8mb4"));
    assertThat(column.getJavaCharset(), is("UTF-8"));
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
    assertThat(column.isFixedLength(), is(false));
    assertThat(column.isVariableLength(), is(true));
    assertThat(column.isVarLenChar(), is(false));
  }

  @Test
  public void testCharColumn() {
    Column column = new Column().setName("c1").setType("CHAR(50)");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getOrdinal(), is(0));
    assertThat(column.getLength(), is(50));
    assertThat(column.getMaxBytesPerChar(), is(1));
    assertThat(column.getPrecision(), is(0));
    assertThat(column.getScale(), is(0));
    assertThat(column.getFullType(), is("CHAR(50)"));
    assertThat(column.getType(), is("CHAR"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), nullValue());
    assertThat(column.getJavaCharset(), nullValue());
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
    assertThat(column.isFixedLength(), is(true));
    assertThat(column.isVariableLength(), is(false));
    assertThat(column.isVarLenChar(), is(false));
  }

  @Test
  public void testCharColumnWithUft8mb4() {
    Column column = new Column().setName("c1").setType("CHAR(50)")
        .setCharset("utf8mb4").setCollation("utf8_bin");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getOrdinal(), is(0));
    assertThat(column.getLength(), is(50));
    assertThat(column.getMaxBytesPerChar(), is(4));
    assertThat(column.getPrecision(), is(0));
    assertThat(column.getScale(), is(0));
    assertThat(column.getFullType(), is("CHAR(50)"));
    assertThat(column.getType(), is("CHAR"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), is("utf8mb4"));
    assertThat(column.getJavaCharset(), is("UTF-8"));
    assertThat(column.getCollation(), is("utf8_bin"));
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
    assertThat(column.isFixedLength(), is(false));
    assertThat(column.isVariableLength(), is(true));
    assertThat(column.isVarLenChar(), is(true));
  }

  @Test
  public void testDatetimeColumn() {
    Column column = new Column().setName("c1").setType("DATETIME(3)");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getOrdinal(), is(0));
    assertThat(column.getLength(), is(0));
    assertThat(column.getMaxBytesPerChar(), is(1));
    assertThat(column.getPrecision(), is(3));
    assertThat(column.getScale(), is(0));
    assertThat(column.getFullType(), is("DATETIME(3)"));
    assertThat(column.getType(), is("DATETIME"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), nullValue());
    assertThat(column.getJavaCharset(), nullValue());
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
  }

  @Test
  public void testDecimalColumn() {
    Column column = new Column().setName("c1").setType("Decimal(9,4)");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getOrdinal(), is(0));
    assertThat(column.getLength(), is(0));
    assertThat(column.getMaxBytesPerChar(), is(1));
    assertThat(column.getPrecision(), is(9));
    assertThat(column.getScale(), is(4));
    assertThat(column.getFullType(), is("Decimal(9,4)"));
    assertThat(column.getType(), is("DECIMAL"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), nullValue());
    assertThat(column.getJavaCharset(), nullValue());
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
  }

  @Test
  public void testDecimalOnlyPrecColumn() {
    Column column = new Column().setName("c1").setType("Decimal(4)");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getLength(), is(0));
    assertThat(column.getMaxBytesPerChar(), is(1));
    assertThat(column.getPrecision(), is(4));
    assertThat(column.getScale(), is(0));
    assertThat(column.getFullType(), is("Decimal(4)"));
    assertThat(column.getType(), is("DECIMAL"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), nullValue());
    assertThat(column.getJavaCharset(), nullValue());
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
  }

  @Test
  public void testDecimalDefaultColumn() {
    Column column = new Column().setName("c1").setType("Decimal");
    System.out.println(column);
    assertThat(column.getName(), is("c1"));
    assertThat(column.getLength(), is(0));
    assertThat(column.getMaxBytesPerChar(), is(1));
    assertThat(column.getPrecision(), is(10));
    assertThat(column.getScale(), is(0));
    assertThat(column.getFullType(), is("Decimal"));
    assertThat(column.getType(), is("DECIMAL"));
    assertThat(column.getTableDef(), nullValue());
    assertThat(column.getCharset(), nullValue());
    assertThat(column.getJavaCharset(), nullValue());
    assertThat(column.getCollation(), nullValue());
    assertThat(column.isNullable(), is(false));
    assertThat(column.isPrimaryKey(), is(false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTypeIsSetBeforeCharset() {
    new Column().setName("c1").setCharset("utf8").setType("Decimal");
  }

  /**
   * This will cause error when parsing columns.
   * Maybe failfast will be better?
   */
  @Test
  public void testNegateType() {
    Column column = new Column().setName("c1").setType("NEGATE");
    assertThat(column.getType(), is("NEGATE"));
  }

}
