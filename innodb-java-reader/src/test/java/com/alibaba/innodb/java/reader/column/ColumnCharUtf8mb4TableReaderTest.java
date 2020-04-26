package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Table charset set to utf8mb4
 *
 * @author xu.zx
 */
public class ColumnCharUtf8mb4TableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef().setDefaultCharset("utf8mb4")
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("varchar(32)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(254)").setNullable(false))
        .addColumn(new Column().setName("d").setType("varchar(255)").setNullable(false))
        .addColumn(new Column().setName("e").setType("varchar(256)").setNullable(false))
        .addColumn(new Column().setName("f").setType("varchar(512)").setNullable(false))
        .addColumn(new Column().setName("g").setType("varchar(768)").setNullable(false))
        .addColumn(new Column().setName("h").setType("varchar(13950)").setNullable(false))
        .addColumn(new Column().setName("i").setType("char(1)").setNullable(false))
        .addColumn(new Column().setName("j").setType("char(32)").setNullable(false))
        .addColumn(new Column().setName("k").setType("char(255)").setNullable(false));
  }

  @Test
  public void testVarcharColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testVarcharColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testVarcharColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(10));

      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        // System.out.println(Arrays.asList(values));

        assertThat(values[0], is(i));

        // test case covers logic for varchar:
        // 1. varlen: len > 127 && max len <= 255
        // 2. len > 768 will use overflow page
        if ((i % 2) == 0) {
          assertThat(record.get("a"), is(((char) (97 + i % 26)) + StringUtils.repeat('阿', 31)));
          assertThat(record.get("b"), is(((char) (97 + i % 26)) + StringUtils.repeat('里', 63)));
          assertThat(record.get("c"), is(((char) (97 + i % 26)) + StringUtils.repeat('巴', 253)));
          assertThat(record.get("d"), is(((char) (97 + i % 26)) + StringUtils.repeat('数', 254)));
          assertThat(record.get("e"), is(((char) (97 + i % 26)) + StringUtils.repeat('据', 255)));
          assertThat(record.get("f"), is(((char) (97 + i % 26)) + StringUtils.repeat('库', 511)));
          assertThat(record.get("g"), is(((char) (97 + i % 26)) + StringUtils.repeat('事', 767)));
          // TODO mysql8.0 lob is not supported
          if (!isMysql8Flag.get()) {
            assertThat(record.get("h"), is(((char) (97 + i % 26)) + StringUtils.repeat('业', 13949)));
          }
          assertThat(record.get("i"), is(String.valueOf((char) (97 + i % 26))));
          assertThat(record.get("j"), is(((char) (97 + i % 26)) + StringUtils.repeat('辰', 31)
              + StringUtils.repeat(' ', 32 - 31 - 1)));
          assertThat(record.get("k"), is(((char) (97 + i % 26)) + StringUtils.repeat('序', 254)
              + StringUtils.repeat(' ', 255 - 254 - 1)));
        } else {
          assertThat(record.get("a"), is(((char) (97 + i % 26)) + StringUtils.repeat('a', 1)));
          assertThat(record.get("b"), is(((char) (97 + i % 26)) + StringUtils.repeat('里', 10)));
          assertThat(record.get("c"), is(((char) (97 + i % 26)) + StringUtils.repeat('b', 126)));
          assertThat(record.get("d"), is(((char) (97 + i % 26)) + StringUtils.repeat('数', 200)));
          assertThat(record.get("e"), is(((char) (97 + i % 26)) + StringUtils.repeat('j', 220)));
          assertThat(record.get("f"), is(((char) (97 + i % 26)) + StringUtils.repeat('库', 400)));
          assertThat(record.get("g"), is(((char) (97 + i % 26)) + StringUtils.repeat('s', 500)));
          if (!isMysql8Flag.get()) {
            assertThat(record.get("h"), is(((char) (97 + i % 26)) + StringUtils.repeat('业', 10000)));
          }
          assertThat(record.get("i"), is(" "));
          assertThat(record.get("j"), is(((char) (97 + i % 26)) + StringUtils.repeat('辰', 10)
              + StringUtils.repeat(' ', 32 - 10 * 3 - 1)));
          assertThat(record.get("k"), is(((char) (97 + i % 26)) + StringUtils.repeat('x', 100)
              + StringUtils.repeat(' ', 255 - 100 - 1)));
        }
      }
    };
  }
}
