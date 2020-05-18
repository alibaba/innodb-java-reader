package com.alibaba.innodb.java.reader.pk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static com.alibaba.innodb.java.reader.Constants.MAX_VAL;
import static com.alibaba.innodb.java.reader.Constants.MIN_VAL;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class CompositePrimaryKeyMultiLevelTableReaderTest extends AbstractTest {

  public String sql() {
    return "CREATE TABLE tb24 (\n"
        + "c1 VARCHAR(100) NOT NULL,\n"
        + "c2 int(10) NOT NULL,\n" // if nullable, then it will be `c2` int(10) NOT NULL DEFAULT '0',
        + "c3 VARCHAR(100) NOT NULL,\n"
        + "c4 VARCHAR(100) NOT NULL,\n"
        + "c5 VARCHAR(100) NOT NULL,\n"
        + "c6 VARCHAR(100) NOT NULL,\n"
        + "c7 timestamp NOT NULL,\n"
        + "c8 VARCHAR(100) NOT NULL,\n"
        + "PRIMARY KEY (c5, c2, c6)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
  }

  //==========================================================================
  // queryAllIterator test
  //==========================================================================

  @Test
  public void testCompositePkMultiLevelMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql())
        .checkQueryAllIterator(expected());

    assertTestOf(this)
        .withMysql56()
        .withSql(sql())
        .checkAllRecordsIs(expected2());
  }

  @Test
  public void testCompositePkMultiLevelMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSql(sql())
        .checkQueryAllIterator(expected());

    assertTestOf(this)
        .withMysql57()
        .withSql(sql())
        .checkAllRecordsIs(expected2());
  }

  @Test
  public void testCompositePkMultiLevelMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSql(sql())
        .checkQueryAllIterator(expected());

    assertTestOf(this)
        .withMysql80()
        .withSql(sql())
        .checkAllRecordsIs(expected2());
  }

  public Consumer<Iterator<GenericRecord>> expected() {
    return iterator -> {
      int count = checkData(iterator, 1000, 2000);
      assertThat(count, is(4000));
    };
  }

  public Consumer<List<GenericRecord>> expected2() {
    return recordList -> {
      int count = checkData(recordList, 1000, 2000);
      assertThat(count, is(4000));
    };
  }

  //==========================================================================
  // range query nothing
  //==========================================================================

  @Test
  public void testRangeQueryIteratorNothingMysql56() {
    testRangeQueryIteratorNothingMysql(a -> a.withMysql56());
  }

  @Test
  public void testRangeQueryIteratorNothingMysql57() {
    testRangeQueryIteratorNothingMysql(a -> a.withMysql57());
  }

  @Test
  public void testRangeQueryIteratorNothingMysql80() {
    testRangeQueryIteratorNothingMysql(a -> a.withMysql80());
  }

  public void testRangeQueryIteratorNothingMysql(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withSql(sql())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of("9999999999", 0, ""), ComparisonOperator.GTE,
            ImmutableList.of("99999999999999999", 0, ""), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withSql(sql())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of("", -1, ""), ComparisonOperator.GTE,
            ImmutableList.of("", 0, ""), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withSql(sql())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of("0", 0, ""), ComparisonOperator.GTE,
            ImmutableList.of("0", 0, ""), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withSql(sql())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.GT,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withSql(sql())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of("9999999999", 0, ""), ComparisonOperator.GTE,
            ImmutableList.of("9999999999", 0, ""), ComparisonOperator.LT);

    assertThat = assertTestOf(this);
    func.accept(assertThat);
    assertThat.withSql(sql())
        .checkRangeQueryIterator(rangeQueryNothingExpected(),
            ImmutableList.of("9999999999", 0, ""), ComparisonOperator.GTE,
            ImmutableList.of("99999999999999999", 0, ""), ComparisonOperator.LT);
  }

  public Consumer<Iterator<GenericRecord>> rangeQueryNothingExpected() {
    return iterator -> assertThat(iterator.hasNext(), is(false));
  }

  //==========================================================================
  // range query partially
  //==========================================================================

  @Test
  public void testRangeQueryIteratorPartiallyMysql56() {
    testCompositePkRangeQueryPartially(a -> a.withMysql56());
  }

  @Test
  public void testRangeQueryIteratorPartiallyMysql57() {
    testCompositePkRangeQueryPartially(a -> a.withMysql57());
  }

  @Test
  public void testRangeQueryIteratorPartiallyMysql80() {
    testCompositePkRangeQueryPartially(a -> a.withMysql80());
  }

  public void testCompositePkRangeQueryPartially(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // no lower
    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1000, 1, 1023 + 1, 4),
            ImmutableList.of("", 0, ""), ComparisonOperator.GTE,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryRecordsIs(expectedRangeQueryPartially2(1000, 1, 1023 + 1, 4),
            ImmutableList.of("", 0, ""), ComparisonOperator.GT,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1000, 1, 1023 + 1, 4),
            ImmutableList.of(), ComparisonOperator.GTE,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1000, 1, 1023 + 1, 4),
            ImmutableList.of(), ComparisonOperator.NOP,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    // mid
    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1002, 3, 1130 + 1, 2),
            ImmutableList.of("1002ooo", 2, "1002ooo"), ComparisonOperator.GTE,
            ImmutableList.of("1130m", 1, "2m"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryRecordsIs(expectedRangeQueryPartially2(1002, 3, 1130 + 1, 2),
            ImmutableList.of("1002ooo", 2, "1002ooo"), ComparisonOperator.GTE,
            ImmutableList.of("1130m", 1, "2m"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryRecordsIs(expectedRangeQueryPartially2(1002, 3, 1130 + 1, 1),
            ImmutableList.of("1002ooo", 2, "1002ooo"), ComparisonOperator.GTE,
            Arrays.asList("1130m"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryRecordsIs(expectedRangeQueryPartially2(1002, 3, 1130 + 2, 1),
            ImmutableList.of("1002ooo", 2, "1002ooo"), ComparisonOperator.GTE,
            Arrays.asList("1130m"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 2),
            Arrays.asList("1773ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 2),
            Arrays.asList("1773ffff"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 3),
            Arrays.asList("1773ffff"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 3, 1803 + 1, 3),
            Arrays.asList("1773ffff", 1, "x"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 3, 1803 + 1, 3),
            Arrays.asList("1773ffff", 2, MIN_VAL), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 2),
            Arrays.asList("1773ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 3),
            Arrays.asList("1773ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 3),
            Arrays.asList("1773a"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 3),
            Arrays.asList("1773a"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 3, 1803 + 1, 3),
            Arrays.asList("1773ffff", 1, "x"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 3, 1803 + 1, 3),
            Arrays.asList("1773ffff", 2, MIN_VAL), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 3),
            Arrays.asList("1773ffff", 2, MAX_VAL), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 4, 1803 + 1, 2),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 4, 1803 + 1, 3),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 4, 1803 + 1, 1),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            Arrays.asList("1803jjjj", 1), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 4, 1803 + 1, 3),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            Arrays.asList("1803jjjj", 1), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 4, 1803 + 1, 1),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            Arrays.asList("1803jjjj", 1, "0"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 4, 1803 + 1, 3),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            Arrays.asList("1803jjjj", 1, "A"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 2),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 3),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 4, 1803 + 1, 3),
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 1),
            ImmutableList.of("1773ffff", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 1),
            ImmutableList.of("1773e", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1368, 3, 1368 + 1, 4),
            ImmutableList.of("1368qqqqqqqqq", 2, "1qqqq"), ComparisonOperator.GTE,
            ImmutableList.of("1368qqqqqqqqq", 2, "2qqqq"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1368, 3, 1368 + 1, 3),
            ImmutableList.of("1368qqqqqqqqq", 2, "1qqqq"), ComparisonOperator.GT,
            ImmutableList.of("1368qqqqqqqqq", 2, "2qqqq"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1368, 4, 1368 + 2, 1),
            ImmutableList.of("1368qqqqqqqqq", 2, "1qqqq"), ComparisonOperator.GT,
            ImmutableList.of("1368qqqqqqqqq", 2, "2qqqq"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1368, 3, 1368 + 2, 1),
            ImmutableList.of("1368qqqqqqqqq", 2, "1qqqq"), ComparisonOperator.GTE,
            ImmutableList.of("1368qqqqqqqqq", 2, "2qqqq"), ComparisonOperator.LTE);

    // mid, nothing
    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1774, 1),
            ImmutableList.of("1773g", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1774gggg", 2, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryRecordsIs(expectedRangeQueryPartially2(1773, 1, 1774, 1),
            ImmutableList.of("1773g", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1774gggg", 2, "1jjjj"), ComparisonOperator.LT);

    // mid, special not exist key
    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 4 + 1),
            ImmutableList.of("1773ffff", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803x", 1, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 1),
            ImmutableList.of("1773ffff", Integer.MAX_VALUE, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1773, 1, 1803 + 1, 4 + 1),
            ImmutableList.of("1773ffff", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", Integer.MAX_VALUE, "1jjjj"), ComparisonOperator.LT);

    // one group
    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1998, 1, 1999 + 1, 1),
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of("1999xxxxxxxxxx", 1, "1999xxxxxxxxxx"), ComparisonOperator.LT);

    // one group + 1 more row
    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1998, 1, 1999 + 1, 2),
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of("1999xxxxxxxxxx", 1, "2xxxxx"), ComparisonOperator.LT);

    // no upper
    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1998, 1, 2000, 4 + 1),
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of("2", 1, ""), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1998, 1, 2000, 4 + 1),
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of("1999xxxxxxxxxx", 2, "3"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1998, 1, 2000, 4 + 1),
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkRangeQueryIterator(expectedRangeQueryPartially(1998, 1, 2000, 4 + 1),
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.NOP);
  }

  /**
   * Test data generator
   */
  public static void main(String[] args) {
    Random random = new Random();
    String tableName = "tb24";
    String col = ""; // (c1,c2,c3)
    String template = "insert into " + tableName + col + " values('%s', %d, "
        + "'%s', '%s', '%s', '%s', '%s', '%s');";

    List<String> list = new ArrayList<>();
    for (int i = 1000; i < 2000; i++) {
      for (int j = 1; j <= 2; j++) {
        for (int k = 1; k <= 2; k++) {
          list.add(String.format(template,
              StringUtils.repeat((char) (97 + i % 26), i % 20 + 1),
              j,
              StringUtils.repeat((char) (97 + i % 26), i % 10 + 1),
              StringUtils.repeat((char) (97 + i % 26), i % 4 + 1),
              i + StringUtils.repeat((char) (97 + i % 26), i % 10 + 1),
              k + StringUtils.repeat((char) (97 + i % 26), i % 5 + 1),
              "2019-10-02 10:59:59",
              RandomStringUtils.randomAlphabetic(4 + random.nextInt(8)))
          );
        }
      }
    }
    Collections.shuffle(list);
    list.forEach(System.out::println);
  }

}
