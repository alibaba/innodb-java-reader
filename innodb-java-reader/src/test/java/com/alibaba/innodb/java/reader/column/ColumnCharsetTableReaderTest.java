package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

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
 * Storage engines may place additional constraints on this limit, reducing the effective maximum row size.
 * <p>
 * 不同的字符集对字段可存储的max会有影响，例如，UTF8字符需要3个字节存储，对于VARCHAR（255）CHARACTER SET UTF8列，会占用255×3 =765的字节。
 * 故该表不能包含超过65,535/765=85这样的列。GBK是双字节的以此类推。
 *
 * @author xu.zx
 */
public class ColumnCharsetTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("varchar(64)").setNullable(false))
        .setCharset("UTF-8");
  }

  @Test
  public void testTableCharsetUtf8mb4Mysql56() {
    testTableCharsetUtf8mb4(IBD_FILE_BASE_PATH_MYSQL56 + "column/char/tb05.ibd");
  }

  @Test
  public void testTableCharsetUtf8mb4Mysql57() {
    testTableCharsetUtf8mb4(IBD_FILE_BASE_PATH_MYSQL57 + "column/char/tb05.ibd");
  }

  //FIXME varchar(9) for 9 chinese characters not working under mysql8? maybe environment issue
  @Ignore
  @Test
  public void testTableCharsetUtf8mb4Mysql80() {
    testTableCharsetUtf8mb4(IBD_FILE_BASE_PATH_MYSQL80 + "column/char/tb05.ibd");
  }

  public void testTableCharsetUtf8mb4(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);

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
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testNegativeTableCharsetUtf8mb4() {
    Schema schema = new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("varchar(64)").setNullable(false))
        .setCharset("ISO-8859-1");
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH_MYSQL56 + "column/char/tb05.ibd", schema)) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);
      System.out.println(recordList.get(0).get("a"));
      if (!"中国".equals(recordList.get(0).get("a"))) {
        throw new IllegalStateException("charset not match");
      }
    }
  }
}
