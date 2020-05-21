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
public class ColumnBitTableReaderTest extends AbstractTest {

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
          new Object[]{1L, "0", "00", "00" + Integer.toBinaryString(31),
              Integer.toBinaryString(438),
              "1111111111111111111111111111111111111111111111111111111111111111"},
          new Object[]{2L, "1", "01", Integer.toBinaryString(119),
              Integer.toBinaryString(368),
              "0000000000000000000000000000000000000000000000000000000000000001"},
          new Object[]{3L, "0", "10", "0" + Integer.toBinaryString(57),
              "0" + Integer.toBinaryString(135),
              "1000000000000000000000000000000000000000000000000000000000000000"},
          new Object[]{4L, "1", "11", "0000" + Integer.toBinaryString(4),
              "0" + Integer.toBinaryString(245),
              "0101010101010101010101010101010101010101010101010101010101010101"});
      System.out.println(Integer.toBinaryString(348));

      for (int i = 0; i < recordList.size(); i++) {
        GenericRecord r = recordList.get(i);
        Object[] v = r.getValues();
        System.out.println(Arrays.asList(v));
        Object[] row = expected.get(i);
        assertThat(r.getPrimaryKey(), is(ImmutableList.of(row[0])));
        assertThat(r.get("a").toString(), is(row[1]));
        assertThat(r.get("b").toString(), is(row[2]));
        assertThat(r.get("c").toString(), is(row[3]));
        assertThat(r.get("d").toString(), is(row[4]));
        assertThat(r.get("e").toString(), is(row[5]));
      }
    };
  }

}
