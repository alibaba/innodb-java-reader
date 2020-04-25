package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnCharLatin1TableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema().setCharset("latin1")
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("varchar(32)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(254)").setNullable(false))
        .addColumn(new Column().setName("d").setType("varchar(255)").setNullable(false))
        .addColumn(new Column().setName("e").setType("varchar(256)").setNullable(false))
        .addColumn(new Column().setName("f").setType("varchar(512)").setNullable(false))
        .addColumn(new Column().setName("g").setType("varchar(16384)").setNullable(false))
        .addColumn(new Column().setName("h").setType("varchar(47474)").setNullable(false))
        .addColumn(new Column().setName("i").setType("char(1)").setNullable(false))
        .addColumn(new Column().setName("j").setType("char(32)").setNullable(false))
        .addColumn(new Column().setName("k").setType("char(255)").setNullable(false));
  }

  @Test
  public void testVarcharColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testVarcharColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testVarcharColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSchema(getSchema())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(10));

      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        // System.out.println(Arrays.asList(values)); // too long

        assertThat(values[0], is(i));

        // test case covers logic for varchar:
        // 1. varlen: len > 127 && max len <= 255
        // 2. len > 768 will use overflow page
        if ((i % 2) == 0) {
          assertThat(record.get("a"), is(((char) (97 + i % 26)) + StringUtils.repeat('a', 31)));
          assertThat(record.get("b"), is(((char) (97 + i % 26)) + StringUtils.repeat('b', 63)));
          assertThat(record.get("c"), is(((char) (97 + i % 26)) + StringUtils.repeat('c', 253)));
          assertThat(record.get("d"), is(((char) (97 + i % 26)) + StringUtils.repeat('d', 254)));
          assertThat(record.get("e"), is(((char) (97 + i % 26)) + StringUtils.repeat('e', 255)));
          assertThat(record.get("f"), is(((char) (97 + i % 26)) + StringUtils.repeat('f', 511)));
          // TODO mysql8.0 lob is not supported
          if (!isMysql8Flag.get()) {
            assertThat(record.get("g"), is(((char) (97 + i % 26)) + StringUtils.repeat('g', 16383)));
            assertThat(record.get("h"), is(((char) (97 + i % 26)) + StringUtils.repeat('h', 47473)));
          }
          assertThat(record.get("i"), is(String.valueOf((char) (97 + i % 26))));
          assertThat(record.get("j"), is(((char) (97 + i % 26)) + StringUtils.repeat('j', 31)
              + StringUtils.repeat(' ', 32 - 31 - 1)));
          assertThat(record.get("k"), is(((char) (97 + i % 26)) + StringUtils.repeat('k', 254)
              + StringUtils.repeat(' ', 255 - 254 - 1)));
        } else {
          assertThat(record.get("a"), is(((char) (97 + i % 26)) + StringUtils.repeat('a', 1)));
          assertThat(record.get("b"), is(((char) (97 + i % 26)) + StringUtils.repeat('b', 10)));
          assertThat(record.get("c"), is(((char) (97 + i % 26)) + StringUtils.repeat('c', 126)));
          assertThat(record.get("d"), is(((char) (97 + i % 26)) + StringUtils.repeat('d', 127)));
          assertThat(record.get("e"), is(((char) (97 + i % 26)) + StringUtils.repeat('e', 128)));
          assertThat(record.get("f"), is(((char) (97 + i % 26)) + StringUtils.repeat('f', 400)));
          if (!isMysql8Flag.get()) {
            assertThat(record.get("g"), is(((char) (97 + i % 26)) + StringUtils.repeat('g', 10000)));
            assertThat(record.get("h"), is(((char) (97 + i % 26)) + StringUtils.repeat('h', 40000)));
          }
          assertThat(record.get("i"), is(" "));
          assertThat(record.get("j"), is(((char) (97 + i % 26)) + StringUtils.repeat('j', 8)
              + StringUtils.repeat(' ', 32 - 8 - 1)));
          assertThat(record.get("k"), is(((char) (97 + i % 26)) + StringUtils.repeat('k', 10)
              + StringUtils.repeat(' ', 255 - 10 - 1)));
        }
      }
    };
  }
}
