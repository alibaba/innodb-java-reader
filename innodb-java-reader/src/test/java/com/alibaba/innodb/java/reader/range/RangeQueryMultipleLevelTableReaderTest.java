package com.alibaba.innodb.java.reader.range;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class RangeQueryMultipleLevelTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(false));
  }

  @Ignore
  @Test
  public void testReadAllPages() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
      reader.open();

      // check read all pages function
      List<AbstractPage> pages = reader.readAllPages();
      for (AbstractPage page : pages) {
        StringBuilder sb = new StringBuilder();
        sb.append(page.getPageNumber()).append(" ");
        sb.append(page.getFilHeader()).append(" ");
        if (page instanceof Index) {
          sb.append(((Index) page).getIndexHeader());
        }
        System.out.println(sb.toString());
      }
    }
  }

  @Test
  public void testMultipleLevelTableRangeQueryAll() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
      reader.open();

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(0, 50001);
      assertThat(recordList.size(), is(40000));
      checkRecords(recordList.subList(0, 20000), 1, 20000);
      //  1 - 20000, 30001 - 50000
      // there is gap in the middle
      checkRecords(recordList.subList(20000, 40000), 30001, 50000);
    }
  }

  @Test
  public void testMultipleLevelTableRangeQueryLowerNothing() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
      reader.open();
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(-1, 0);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(0, 0);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(50001, 50001);
      assertThat(recordList.size(), is(0));

      recordList = reader.rangeQueryByPrimaryKey(50001, 60001);
      assertThat(recordList.size(), is(0));
    }
  }

  @Test
  public void testMultipleLevelTableRangeQueryPart() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
      reader.open();
      rangeQuery(reader, 3000, 8000);
      rangeQuery(reader, 10000, 19999);
      rangeQuery(reader, 6000, 6000);
    }
  }

  private void rangeQuery(TableReader reader, int start, int end) {
    List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(start, end);
    if (end == start) {
      end++;
    }
    assertThat(recordList.size(), is(end - start));
    int index = 0;
    for (int i = start; i < end; i++) {
      GenericRecord record = recordList.get(index++);
      Object[] values = record.getValues();
      //System.out.println(Arrays.asList(values));
      assertThat(values[0], is(i));
      assertThat(record.get("a"), is(i * 2L));
      assertThat(record.get("b"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 32))));
      assertThat(record.get("c"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 512))));
    }
  }

  @Test
  public void testSimpleTableRangeQueryHalfOpenHalfClose() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
      reader.open();

      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(0, null);
      assertThat(recordList.size(), is(40000));

      recordList = reader.rangeQueryByPrimaryKey(1, null);
      assertThat(recordList.size(), is(40000));

      recordList = reader.rangeQueryByPrimaryKey(5, null);
      assertThat(recordList.size(), is(39996));

      recordList = reader.rangeQueryByPrimaryKey(null, 100);
      assertThat(recordList.size(), is(99));

      recordList = reader.rangeQueryByPrimaryKey(null, 6);
      assertThat(recordList.size(), is(5));

      recordList = reader.rangeQueryByPrimaryKey(null, 1);
      assertThat(recordList.size(), is(0));
    }
  }

  private void checkRecords(List<GenericRecord> recordList, int start, int end) {
    int index = 0;
    for (int i = start; i <= end; i++) {
      GenericRecord record = recordList.get(index++);
      Object[] values = record.getValues();
      //System.out.println(Arrays.asList(values));
      assertThat(values[0], is(i));
      assertThat(record.get("a"), is(i * 2L));
      assertThat(record.get("b"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 32))));
      assertThat(record.get("c"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 512))));
    }
  }
}
