package com.alibaba.innodb.java.reader.column;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ColumnEnumTableReaderTest extends AbstractTest {

  @Test
  public void testIntColumnMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDefProvider()
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testIntColumnMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDefProvider()
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testIntColumnMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDefProvider()
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(4));

      List<Object[]> expected = Arrays.asList(
          new Object[]{1L, "A", "MYSQL", "数据", "001019"},
          new Object[]{2L, "C", "computer", "数据", "001001"},
          new Object[]{3L, "B", "world", "存储", "803019"},
          new Object[]{4L, "0xE4", "Hello", "存储", "429002"});

      for (int i = 0; i < recordList.size(); i++) {
        GenericRecord r = recordList.get(i);
        Object[] v = r.getValues();
        System.out.println(Arrays.asList(v));
        Object[] row = expected.get(i);
        assertThat(r.getPrimaryKey(), is(ImmutableList.of(row[0])));
        assertThat(r.get("a").toString(), is(row[1]));
        assertThat(r.get("b").toString(), is(row[2]));
        assertThat(r.get("c").toString(), is(row[3]));
      }
    };
  }

}
