package com.alibaba.innodb.java.reader.pk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class FirstUniqueKeyAsPrimaryKeyTableReaderTest extends AbstractTest {

  String sql = "CREATE TABLE `tb28`(\n"
      + "`a` int(11) NOT NULL,\n"
      + "`b` varchar(10) NOT NULL,\n"
      + "`c` varchar(10) NOT NULL,\n"
      + "`d` varchar(10) DEFAULT '',\n"
      + "`e` varchar(10) NOT NULL,\n"
      + "UNIQUE KEY `key_d` (`d`),\n"
      + "UNIQUE KEY `key_e_d` (`e`, `d`),\n"
      + "KEY `key_e` (`e`),\n"
      + "KEY `key_a` (`a`),\n"
      + "UNIQUE KEY `key_b` (`b`),\n"
      + "KEY `key_c` (`c`)\n"
      + ")\n"
      + "ENGINE=InnoDB;";

  @Test
  public void testFirstNonNullUniqueKeyAsPkMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testFirstNonNullUniqueKeyAsPkMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSql(sql)
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testFirstNonNullUniqueKeyAsPkMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSql(sql)
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testFirstNonNullUniqueKeyAsPQueryAllIteratorMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryAllIterator(expectedIterator(true));
  }

  @Test
  public void testFirstNonNullUniqueKeyAsPQueryAllIteratorMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSql(sql)
        .checkQueryAllIterator(expectedIterator(true));
  }

  @Test
  public void testFirstNonNullUniqueKeyAsPQueryAllIteratorMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSql(sql)
        .checkQueryAllIterator(expectedIterator(true));
  }

  @Test
  public void testFirstNonNullUniqueKeyAsPQueryAllIteratorDescMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryAllIteratorDesc(expectedIterator(false));
  }

  @Test
  public void testFirstNonNullUniqueKeyAsPQueryAllIteratorDescMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSql(sql)
        .checkQueryAllIteratorDesc(expectedIterator(false));
  }

  @Test
  public void testFirstNonNullUniqueKeyAsPQueryAllIteratorDescMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSql(sql)
        .checkQueryAllIteratorDesc(expectedIterator(false));
  }

  /**
   * column d is primary key in lexicographical order
   */
  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      int expectSize = 40;
      assertThat(recordList.size(), is(expectSize));

      List<String> expectedIdList = new ArrayList<>();
      for (int i = 1; i <= expectSize; i++) {
        expectedIdList.add(String.valueOf(i));
      }
      Collections.sort(expectedIdList);
      System.out.println(expectedIdList);

      for (int i = 1; i <= expectSize; i++) {
        GenericRecord r = recordList.get(i - 1);
        Object[] v = r.getValues();
        System.out.println(Arrays.asList(v));
        int id = Integer.parseInt(expectedIdList.get(i - 1));
        assertThat(r.getPrimaryKey().isEmpty(), is(false));
        assertThat(r.getTableDef().getPrimaryKeyColumnNames(), is(ImmutableList.of("b")));
        assertThat(r.getPrimaryKey(), is(ImmutableList.of("bb" + id)));
        assertThat(r.get("a"), is(id));
        assertThat(r.get("b"), is("bb" + id));
        assertThat(r.get("c"), is("cc" + id));
        assertThat(r.get("d"), is("DD" + id));
        assertThat(r.get("e"), is("EE" + id));
      }
    };
  }

  public Consumer<Iterator<GenericRecord>> expectedIterator(boolean asc) {
    return iterator -> {

      assertThat(iterator.hasNext(), is(true));
      List<GenericRecord> list = new ArrayList<>();
      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      if (!asc) {
        Collections.reverse(list);
      }
      expected().accept(list);
    };
  }

}
