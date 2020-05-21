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
public class ColumnSetTableReaderTest extends AbstractTest {

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

      assertThat(recordList.size(), is(3));

      List<Object[]> expected = Arrays.asList(
          new Object[]{1, "music", "a,e,i,o,u", "3"},
          new Object[]{2, "movie,swimming", "o,p,q", "1,5,60"},
          new Object[]{3, "movie,足球", "z", "1,2,3,4,5,6,7,8,9,10,11,12,13,14,24,31,33,"
              + "37,48,49,50,55,63,64"});

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
