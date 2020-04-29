package com.alibaba.innodb.java.reader.pk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderImpl;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class CompositePrimaryKeyTableReaderTest extends AbstractTest {

  public String sql() {
    return "CREATE TABLE tb23 (\n"
        + "c1 VARCHAR(30) NOT NULL,\n"
        + "c2 VARCHAR(30),\n"
        + "c3 VARCHAR(30) NOT NULL,\n"
        + "c4 VARCHAR(30),\n"
        + "c5 VARCHAR(30) NOT NULL,\n"
        + "c6 VARCHAR(30),\n"
        + "c7 VARCHAR(30) NOT NULL,\n"
        + "c8 VARCHAR(30),\n"
        + "c9 VARCHAR(30) NOT NULL,\n"
        + "c10 VARCHAR(30),\n"
        + "c11 VARCHAR(30) NOT NULL,\n"
        + "c12 VARCHAR(30),\n"
        + "PRIMARY KEY (c5, c3, c9)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
  }

  //==========================================================================
  // query all test
  //==========================================================================

  @Test
  public void testCompositePkMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testCompositePkMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSql(sql())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testCompositePkMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSql(sql())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(3));

      GenericRecord r1 = recordList.get(0);
      checkRecord1(r1);

      GenericRecord r2 = recordList.get(1);
      checkRecord2(r2);

      GenericRecord r3 = recordList.get(2);
      checkRecord3(r3);
    };
  }

  //==========================================================================
  // queryByPrimaryKey test
  //==========================================================================

  @Test
  public void testQueryCompositePkMysql56() {
    testQueryCompositePk(IBD_FILE_BASE_PATH_MYSQL56 + "pk/tb23.ibd");
  }

  @Test
  public void testQueryCompositePkMysql57() {
    testQueryCompositePk(IBD_FILE_BASE_PATH_MYSQL57 + "pk/tb23.ibd");
  }

  @Test
  public void testQueryCompositePkMysql80() {
    testQueryCompositePk(IBD_FILE_BASE_PATH_MYSQL80 + "pk/tb23.ibd");
  }

  public void testQueryCompositePk(String path) {
    try (TableReader reader = new TableReaderImpl(path, sql())) {
      reader.open();

      GenericRecord record = reader.queryByPrimaryKey(
          ImmutableList.of(
              val(5, 'a', 5),
              val(3, 'a', 3),
              val(9, 'a', 9)));
      checkRecord1(record);

      record = reader.queryByPrimaryKey(
          ImmutableList.of(
              val(5, 'b', 5),
              val(3, 'b', 3),
              val(9, 'b', 9)));
      checkRecord2(record);

      record = reader.queryByPrimaryKey(
          ImmutableList.of(
              val(5, 'c', 5),
              val(3, 'c', 3),
              val(9, 'c', 9)));
      checkRecord3(record);

      record = reader.queryByPrimaryKey(
          ImmutableList.of(
              val(5, 'c', 5),
              val(3, 'c', 3),
              val(9, 'c', 10)));
      assertThat(record, nullValue());
    }
  }

  private void checkRecord1(GenericRecord r1) {
    Object[] v1 = r1.getValues();
    System.out.println(Arrays.asList(v1));
    assertThat(r1.getPrimaryKey(), is(
        ImmutableList.of(
            val(5, 'a', 5),
            val(3, 'a', 3),
            val(9, 'a', 9)
        )));
    List<Integer> nullCols = ImmutableList.of(2, 8);
    for (int i = 1; i <= 9; i++) {
      if (nullCols.contains(i)) {
        assertThat(r1.get("c" + i), nullValue());
      } else {
        assertThat(r1.get("c" + i), is(val(i, 'a', i)));
      }
    }
    assertThat(r1.get("c10"), is(val('x', 'a', 10)));
    assertThat(r1.get("c11"), is(val('y', 'a', 11)));
    assertThat(r1.get("c12"), is(val('z', 'a', 12)));
  }

  private void checkRecord2(GenericRecord r2) {
    List<Integer> nullCols;
    Object[] v2 = r2.getValues();
    System.out.println(Arrays.asList(v2));
    assertThat(r2.getPrimaryKey(), is(
        ImmutableList.of(
            val(5, 'b', 5),
            val(3, 'b', 3),
            val(9, 'b', 9)
        )));
    nullCols = ImmutableList.of(4, 6, 10);
    for (int i = 1; i <= 9; i++) {
      if (nullCols.contains(i)) {
        assertThat(r2.get("c" + i), nullValue());
      } else {
        assertThat(r2.get("c" + i), is(val(i, 'b', i)));
      }
    }
    assertThat(r2.get("c10"), is(val('x', 'b', 10)));
    assertThat(r2.get("c11"), is(val('y', 'b', 11)));
    assertThat(r2.get("c12"), nullValue());
  }

  private void checkRecord3(GenericRecord r3) {
    List<Integer> nullCols;
    Object[] v3 = r3.getValues();
    System.out.println(Arrays.asList(v3));
    assertThat(r3.getPrimaryKey(), is(
        ImmutableList.of(
            val(5, 'c', 5),
            val(3, 'c', 3),
            val(9, 'c', 9)
        )));
    nullCols = ImmutableList.of(4, 6, 12);
    for (int i = 1; i <= 9; i++) {
      if (nullCols.contains(i)) {
        assertThat(r3.get("c" + i), nullValue());
      } else {
        assertThat(r3.get("c" + i), is(val(i, 'c', i)));
      }
    }
    assertThat(r3.get("c10"), nullValue());
    assertThat(r3.get("c11"), is(val('y', 'c', 11)));
    assertThat(r3.get("c12"), is(val('z', 'c', 12)));
  }

  private String val(int i, char c, int count) {
    return i + StringUtils.repeat(c, count);
  }

  private String val(char i, char c, int count) {
    return i + StringUtils.repeat(c, count);
  }
}
