package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * insert into tb03 values(null, 100, '2019-10-02 10:59:59', '2019-10-02 10:59:59');
 * insert into tb03 values(null, 101, '1970-01-01 08:00:01', '1970-01-01 08:00:01');
 * insert into tb03 values(null, 102, '2008-11-23 09:23:00', '2008-11-23 09:23:00');
 *
 * @author xu.zx
 */
public class ColumnTimeTableReaderTest extends AbstractTest {

  DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("int(11)").setNullable(false))
        .addColumn(new Column().setName("b").setType("datetime").setNullable(false))
        .addColumn(new Column().setName("c").setType("timestamp").setNullable(false));
  }

  @Test
  public void testTimeColumnMysql56() {
    testTimeColumn(IBD_FILE_BASE_PATH_MYSQL56 + "column/time/tb03.ibd");
  }

  @Test
  public void testTimeColumnMysql57() {
    testTimeColumn(IBD_FILE_BASE_PATH_MYSQL57 + "column/time/tb03.ibd");
  }

  @Test
  public void testTimeColumnMysql80() {
    testTimeColumn(IBD_FILE_BASE_PATH_MYSQL80 + "column/time/tb03.ibd");
  }

  public void testTimeColumn(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(3);

      assertThat(recordList.size(), is(3));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey(), is(1));
      assertThat(r1.get("a"), is(100));
      assertThat(r1.get("b"), is(formatter.parseDateTime("2019-10-02 10:59:59").toDate()));
      // FIXME not working in travis
      // assertThat(r1.get("c"), is(formatter.parseDateTime("2019-10-02 10:59:59").getMillis()));
      assertThat(((Timestamp) r1.get("c")).getTime() >= 1569985199000L, is(true));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey(), is(2));
      assertThat(r2.get("a"), is(101));
      assertThat(r2.get("b"), is(formatter.parseDateTime("1970-01-01 08:00:01").toDate()));
      //assertThat(((Timestamp) r2.get("c")).getTime(), is(formatter.parseDateTime("1970-01-01 08:00:01").getMillis()));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey(), is(3));
      assertThat(r3.get("a"), is(102));
      assertThat(r3.get("b"), is(formatter.parseDateTime("2008-11-23 09:23:00").toDate()));
      //assertThat(((Timestamp) r3.get("c")).getTime(), is(formatter.parseDateTime("2008-11-23 09:23:00").getMillis()));
    }
  }
}
