package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * <pre>
 * mysql> select  * from tb05;
 * +----+--------------------------+
 * | id | a                        |
 * +----+--------------------------+
 * |  1 | 中国                     |
 * |  2 | 你好这里是哪里           |
 * |  3 | 我爱你                   |
 * |  4 | 千里之行始于足下         |
 * |  5 | 不积跬步无以至千里         |
 * +----+--------------------------+
 * </pre>
 *
 * “65535”不是单个varchar(N)中N的最大限制，而是整个表非大字段类型的字段的bytes总合。
 * Every table (regardless of storage engine) has a maximum row size of 65,535 bytes.
 * Storage engines may place additional constraints on this limit, reducing the effective
 * maximum row size.
 * <p>
 * 不同的字符集对字段可存储的max会有影响，例如，UTF8字符需要3个字节存储，对于VARCHAR（255）CHARACTER SET UTF8列，
 * 会占用255×3 =765的字节。故该表不能包含超过65,535/765=85这样的列。GBK是双字节的以此类推。
 *
 * @author xu.zx
 */
public class ColumnCharSimpleTableCharsetTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef().setDefaultCharset("utf8")
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("varchar(64)").setNullable(false));
  }

  @Test
  public void testTableCharsetMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testTableCharsetMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testTableCharsetMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(5));

      assertThat(recordList.get(0).get("id"), is(1));
      assertThat(recordList.get(0).get("a"), is("中国"));

      assertThat(recordList.get(1).get("id"), is(2));
      assertThat(recordList.get(1).get("a"), is("你好这里是哪里"));

      assertThat(recordList.get(2).get("id"), is(3));
      assertThat(recordList.get(2).get("a"), is("我爱你"));

      assertThat(recordList.get(3).get("id"), is(4));
      assertThat(recordList.get(3).get("a"), is("千里之行始于足下"));

      assertThat(recordList.get(4).get("id"), is(5));
      assertThat(recordList.get(4).get("a"), is("不积跬步无以至千里"));
    };
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNegativeNotExistTableCharset() {
    TableDef tableDef = new TableDef().setDefaultCharset("wrong_charset")
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("varchar(64)").setNullable(false));
  }

  @Test(expected = IllegalStateException.class)
  public void testNegativeWonrgTableCharset() {
    TableDef tableDef = new TableDef().setDefaultCharset("ucs2")
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("varchar(64)").setNullable(false));
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH_MYSQL56 + "column/char/tb05.ibd", tableDef)) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);
      System.out.println(recordList.get(0).get("a"));
      if (!"中国".equals(recordList.get(0).get("a"))) { // should not match
        throw new IllegalStateException("charset not match");
      }
    }
    fail();
  }
}
