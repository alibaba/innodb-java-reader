package com.alibaba.innodb.java.reader.pk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.apache.commons.lang3.StringUtils;
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
public class NoPrimaryKeyTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("a").setType("int(11)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(10)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(10)").setNullable(false));
  }

  @Test
  public void testNoPkMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testNoPkMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testNoPkMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testNoPkQueryAllIteratorMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkQueryAllIterator(expectedIterator(true));
  }

  @Test
  public void testNoPkQueryAllIteratorMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkQueryAllIterator(expectedIterator(true));
  }

  @Test
  public void testNoPkQueryAllIteratorMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkQueryAllIterator(expectedIterator(true));
  }

  @Test
  public void testNoPkQueryAllIteratorDescMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkQueryAllIteratorDesc(expectedIterator(false));
  }

  @Test
  public void testNoPkQueryAllIteratorDescMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkQueryAllIteratorDesc(expectedIterator(false));
  }

  @Test
  public void testNoPkQueryAllIteratorDescMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkQueryAllIteratorDesc(expectedIterator(false));
  }

  /**
   * Querying by primary key is not allowed.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testQueryByPk() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkQueryByPk(record -> {
        }, ImmutableList.of(100));
  }

  /**
   * Querying by primary key will be treated as query all.
   */
  @Test
  public void testRangeQueryByPk56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(expectedIterator(true),
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT);
  }

  @Test
  public void testRangeQueryByPk57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(expectedIterator(true),
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT);
  }

  @Test
  public void testRangeQueryByPk80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkRangeQueryIterator(expectedIterator(true),
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT);
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(10));

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey().isEmpty(), is(true));
      assertThat(r1.get("a"), is(600));
      assertThat(r1.get("b"), is("Jason"));
      assertThat(r1.get("c"), is(StringUtils.repeat("a", 9)));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey().isEmpty(), is(true));
      assertThat(r2.get("a"), is(900));
      assertThat(r2.get("b"), is("Eric"));
      assertThat(r2.get("c"), is(StringUtils.repeat("b", 8)));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey().isEmpty(), is(true));
      assertThat(r3.get("a"), is(1000));
      assertThat(r3.get("b"), is("Tom"));
      assertThat(r3.get("c"), is(StringUtils.repeat("c", 7)));

      GenericRecord r4 = recordList.get(3);
      Object[] v4 = r4.getValues();
      System.out.println(Arrays.asList(v4));
      assertThat(r4.getPrimaryKey().isEmpty(), is(true));
      assertThat(r4.get("a"), is(500));
      assertThat(r4.get("b"), is("Sarah"));
      assertThat(r4.get("c"), is(StringUtils.repeat("d", 6)));

      GenericRecord r5 = recordList.get(4);
      Object[] v5 = r5.getValues();
      System.out.println(Arrays.asList(v5));
      assertThat(r5.getPrimaryKey().isEmpty(), is(true));
      assertThat(r5.get("a"), is(400));
      assertThat(r5.get("b"), is("jim"));
      assertThat(r5.get("c"), is(StringUtils.repeat("e", 5)));

      GenericRecord r6 = recordList.get(5);
      Object[] v6 = r6.getValues();
      System.out.println(Arrays.asList(v6));
      assertThat(r6.getPrimaryKey().isEmpty(), is(true));
      assertThat(r6.get("a"), is(100));
      assertThat(r6.get("b"), is("tom"));
      assertThat(r6.get("c"), is(StringUtils.repeat("f", 4)));

      GenericRecord r7 = recordList.get(6);
      Object[] v7 = r7.getValues();
      System.out.println(Arrays.asList(v7));
      assertThat(r7.getPrimaryKey().isEmpty(), is(true));
      assertThat(r7.get("a"), is(200));
      assertThat(r7.get("b"), is("jim"));
      assertThat(r7.get("c"), is(StringUtils.repeat("g", 3)));

      GenericRecord r8 = recordList.get(7);
      Object[] v8 = r8.getValues();
      System.out.println(Arrays.asList(v8));
      assertThat(r8.getPrimaryKey().isEmpty(), is(true));
      assertThat(r8.get("a"), is(800));
      assertThat(r8.get("b"), is("Lucy"));
      assertThat(r8.get("c"), is(StringUtils.repeat("h", 2)));

      GenericRecord r9 = recordList.get(8);
      Object[] v9 = r9.getValues();
      System.out.println(Arrays.asList(v9));
      assertThat(r9.getPrimaryKey().isEmpty(), is(true));
      assertThat(r9.get("a"), is(700));
      assertThat(r9.get("b"), is("smith"));
      assertThat(r9.get("c"), is(StringUtils.repeat("i", 1)));

      GenericRecord r10 = recordList.get(9);
      Object[] v10 = r10.getValues();
      System.out.println(Arrays.asList(v10));
      assertThat(r10.getPrimaryKey().isEmpty(), is(true));
      assertThat(r10.get("a"), is(300));
      assertThat(r10.get("b"), is("jane"));
      assertThat(r10.get("c"), is(StringUtils.repeat("j", 8)));
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
