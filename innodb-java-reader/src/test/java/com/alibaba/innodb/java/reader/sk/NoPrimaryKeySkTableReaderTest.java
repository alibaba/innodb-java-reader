package com.alibaba.innodb.java.reader.sk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static com.alibaba.innodb.java.reader.Constants.COLUMN_ROW_ID;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class NoPrimaryKeySkTableReaderTest extends AbstractTest {

  String sql = "CREATE TABLE `tb21`(\n"
      + "`a` int(11) NOT NULL,\n"
      + "`b` varchar(10) NOT NULL,\n"
      + "`c` varchar(10) NOT NULL,\n"
      + "KEY `key_b` (`b`),\n"
      + "KEY `key_a` (`a`)\n"
      + ")\n"
      + "ENGINE=InnoDB;";

  @Test
  public void testNoPkSkBQueryAllIteratorMysql56() {
    testQueryBySkB(a -> a.withMysql56());
  }

  @Test
  public void testNoPkSkBQueryAllIteratorMysql57() {
    testQueryBySkB(a -> a.withMysql57());
  }

  @Test
  public void testNoPkSkBQueryAllIteratorMysql80() {
    testQueryBySkB(a -> a.withMysql80());
  }

  public void testQueryBySkB(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(expectedIterator(true),
            "key_b",
            ImmutableList.of("a"), ComparisonOperator.GTE,
            ImmutableList.of("z"), ComparisonOperator.LT);
  }

  @Test
  public void testNoPkSkAQueryAllIteratorMysql56() {
    testQueryBySkA(a -> a.withMysql56());
  }

  @Test
  public void testNoPkSkAQueryAllIteratorMysql57() {
    testQueryBySkA(a -> a.withMysql57());
  }

  @Test
  public void testNoPkSkAQueryAllIteratorMysql80() {
    testQueryBySkA(a -> a.withMysql80());
  }

  public void testQueryBySkA(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(expectedIterator(false),
            "key_a",
            ImmutableList.of("50"), ComparisonOperator.GTE,
            ImmutableList.of("1001"), ComparisonOperator.LT);

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(expectedIterator(false),
            "key_a",
            ImmutableList.of("50"), ComparisonOperator.GT,
            ImmutableList.of(1000), ComparisonOperator.LTE);

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(expectedIterator(false),
            "key_a",
            ImmutableList.of(100), ComparisonOperator.GTE,
            ImmutableList.of("1000"), ComparisonOperator.LTE);

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(expectedIterator(false),
            "key_a",
            ImmutableList.of("50"), ComparisonOperator.GT,
            ImmutableList.of("1001"), ComparisonOperator.LT);
  }

  public Consumer<List<GenericRecord>> expected(boolean checkColumnB) {
    return recordList -> {

      assertThat(recordList.size(), is(10));
      if (checkColumnB) {
        // verify sort by b
        // mysql> select b from tb21 force index (key_b) ;
        //+-------+
        //| b     |
        //+-------+
        //| Eric  |
        //| jane  |
        //| Jason |
        //| jim   |
        //| jim   |
        //| Lucy  |
        //| Sarah |
        //| smith |
        //| Tom   |
        //| tom   |
        //+-------+
        List<String> bList = Arrays.asList("Eric",
            "jane",
            "Jason",
            "jim",
            "jim",
            "Lucy",
            "Sarah",
            "smith",
            "Tom",
            "tom");
        for (int i = 0; i < bList.size(); i++) {
          assertThat((String) recordList.get(i).get("b"), is(bList.get(i)));
        }
      } else {
        // verify sort by a
        // mysql> select a from tb21 force index (key_a) ;
        //+------+
        //| a    |
        //+------+
        //|  100 |
        //|  200 |
        //|  300 |
        //|  400 |
        //|  500 |
        //|  600 |
        //|  700 |
        //|  800 |
        //|  900 |
        //| 1000 |
        //+------+
        List<Integer> aList = Arrays.asList(100, 200, 300, 400, 500, 600, 700, 800, 900, 1000);
        for (int i = 0; i < aList.size(); i++) {
          assertThat(recordList.get(i).get("a"), is(aList.get(i)));
        }
      }

      // verify record
      Collections.sort(recordList, new Comparator<GenericRecord>() {
        @Override
        public int compare(GenericRecord o1, GenericRecord o2) {
          return Long.compare((long) o1.get(COLUMN_ROW_ID), (long) o2.get(COLUMN_ROW_ID));
        }
      });

      long rowId = (long) recordList.get(0).get(COLUMN_ROW_ID);
      for (int i = 1; i < recordList.size(); i++) {
        assertThat(recordList.get(i).get(COLUMN_ROW_ID), is(++rowId));
      }

      GenericRecord r1 = recordList.get(0);
      Object[] v1 = r1.getValues();
      System.out.println(Arrays.asList(v1));
      assertThat(r1.getPrimaryKey().isEmpty(), is(false));
      assertThat(r1.get("a"), is(600));
      assertThat(r1.get("b"), is("Jason"));
      assertThat(r1.get("c"), is(StringUtils.repeat("a", 9)));

      GenericRecord r2 = recordList.get(1);
      Object[] v2 = r2.getValues();
      System.out.println(Arrays.asList(v2));
      assertThat(r2.getPrimaryKey().isEmpty(), is(false));
      assertThat(r2.get("a"), is(900));
      assertThat(r2.get("b"), is("Eric"));
      assertThat(r2.get("c"), is(StringUtils.repeat("b", 8)));

      GenericRecord r3 = recordList.get(2);
      Object[] v3 = r3.getValues();
      System.out.println(Arrays.asList(v3));
      assertThat(r3.getPrimaryKey().isEmpty(), is(false));
      assertThat(r3.get("a"), is(1000));
      assertThat(r3.get("b"), is("Tom"));
      assertThat(r3.get("c"), is(StringUtils.repeat("c", 7)));

      GenericRecord r4 = recordList.get(3);
      Object[] v4 = r4.getValues();
      System.out.println(Arrays.asList(v4));
      assertThat(r4.getPrimaryKey().isEmpty(), is(false));
      assertThat(r4.get("a"), is(500));
      assertThat(r4.get("b"), is("Sarah"));
      assertThat(r4.get("c"), is(StringUtils.repeat("d", 6)));

      GenericRecord r5 = recordList.get(4);
      Object[] v5 = r5.getValues();
      System.out.println(Arrays.asList(v5));
      assertThat(r5.getPrimaryKey().isEmpty(), is(false));
      assertThat(r5.get("a"), is(400));
      assertThat(r5.get("b"), is("jim"));
      assertThat(r5.get("c"), is(StringUtils.repeat("e", 5)));

      GenericRecord r6 = recordList.get(5);
      Object[] v6 = r6.getValues();
      System.out.println(Arrays.asList(v6));
      assertThat(r6.getPrimaryKey().isEmpty(), is(false));
      assertThat(r6.get("a"), is(100));
      assertThat(r6.get("b"), is("tom"));
      assertThat(r6.get("c"), is(StringUtils.repeat("f", 4)));

      GenericRecord r7 = recordList.get(6);
      Object[] v7 = r7.getValues();
      System.out.println(Arrays.asList(v7));
      assertThat(r7.getPrimaryKey().isEmpty(), is(false));
      assertThat(r7.get("a"), is(200));
      assertThat(r7.get("b"), is("jim"));
      assertThat(r7.get("c"), is(StringUtils.repeat("g", 3)));

      GenericRecord r8 = recordList.get(7);
      Object[] v8 = r8.getValues();
      System.out.println(Arrays.asList(v8));
      assertThat(r8.getPrimaryKey().isEmpty(), is(false));
      assertThat(r8.get("a"), is(800));
      assertThat(r8.get("b"), is("Lucy"));
      assertThat(r8.get("c"), is(StringUtils.repeat("h", 2)));

      GenericRecord r9 = recordList.get(8);
      Object[] v9 = r9.getValues();
      System.out.println(Arrays.asList(v9));
      assertThat(r9.getPrimaryKey().isEmpty(), is(false));
      assertThat(r9.get("a"), is(700));
      assertThat(r9.get("b"), is("smith"));
      assertThat(r9.get("c"), is(StringUtils.repeat("i", 1)));

      GenericRecord r10 = recordList.get(9);
      Object[] v10 = r10.getValues();
      System.out.println(Arrays.asList(v10));
      assertThat(r10.getPrimaryKey().isEmpty(), is(false));
      assertThat(r10.get("a"), is(300));
      assertThat(r10.get("b"), is("jane"));
      assertThat(r10.get("c"), is(StringUtils.repeat("j", 8)));
    };
  }

  public Consumer<Iterator<GenericRecord>> expectedIterator(boolean checkColumnB) {
    return iterator -> {

      assertThat(iterator.hasNext(), is(true));
      List<GenericRecord> list = new ArrayList<>();
      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      expected(checkColumnB).accept(list);
    };
  }

}
