package com.alibaba.innodb.java.reader.sk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static com.alibaba.innodb.java.reader.Constants.MAX_VAL;
import static com.alibaba.innodb.java.reader.Constants.MIN_VAL;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class MultipleLevelSkTableReaderTest extends AbstractTest {

  public String sql() {
    return "CREATE TABLE `tb24_sk` (\n"
        + "  `c1` varchar(100) NOT NULL,\n"
        + "  `c2` int(10) NOT NULL DEFAULT '0',\n"
        + "  `c3` varchar(100) NOT NULL,\n"
        + "  `c4` varchar(100) NOT NULL,\n"
        + "  `c5` varchar(100) NOT NULL,\n"
        + "  `c6` varchar(100) NOT NULL,\n"
        + "  `c7` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "  `c8` varchar(100) NOT NULL,\n"
        + "  PRIMARY KEY (`c8`),\n"
        + "  KEY `mykey` (`c5`,`c2`,`c6`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
  }

  //==========================================================================
  // range query partially
  //==========================================================================

  @Test
  public void testQueryBySkPartiallyMysql56() {
    testQueryBySkPartially(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkPartiallyMysql57() {
    testQueryBySkPartially(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkPartiallyMysql80() {
    testQueryBySkPartially(a -> a.withMysql80());
  }

  public void testQueryBySkPartially(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // no lower
    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1000, 1, 1023 + 1, 4),
            "mykey",
            ImmutableList.of("", 0, ""), ComparisonOperator.GTE,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1000, 1, 1023 + 1, 4),
            "mykey",
            ImmutableList.of("", "0", ""), ComparisonOperator.GTE,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1000, 1, 1023 + 1, 4),
            "mykey",
            ImmutableList.of("", 0, ""), ComparisonOperator.GT,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1000, 1, 1023 + 1, 4),
            "mykey",
            ImmutableList.of(), ComparisonOperator.GTE,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1000, 1, 1023 + 1, 4),
            "mykey",
            ImmutableList.of(), ComparisonOperator.NOP,
            ImmutableList.of("1023jjjj", 2, "2jjjj"), ComparisonOperator.LT);

    // mid
    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1002, 3, 1130 + 1, 2),
            "mykey",
            ImmutableList.of("1002ooo", 2, "1002ooo"), ComparisonOperator.GTE,
            ImmutableList.of("1130m", 1, "2m"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1002, 3, 1130 + 1, 2),
            "mykey",
            ImmutableList.of("1002ooo", "2", "1002ooo"), ComparisonOperator.GTE,
            ImmutableList.of("1130m", "1", "2m"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1002, 3, 1130 + 1, 2),
            "mykey",
            ImmutableList.of("1002ooo", 2, "1002ooo"), ComparisonOperator.GTE,
            ImmutableList.of("1130m", 1, "2m"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1002, 3, 1130 + 1, 1),
            "mykey",
            ImmutableList.of("1002ooo", 2, "1002ooo"), ComparisonOperator.GTE,
            Arrays.asList("1130m"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1002, 3, 1130 + 2, 1),
            "mykey",
            ImmutableList.of("1002ooo", 2, "1002ooo"), ComparisonOperator.GTE,
            Arrays.asList("1130m"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 2),
            "mykey",
            Arrays.asList("1773ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 2),
            "mykey",
            Arrays.asList("1773ffff"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773ffff"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 3, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773ffff", 1, "x"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 3, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773ffff", 2, MIN_VAL), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 2),
            "mykey",
            Arrays.asList("1773ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773a"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773a"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 3, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773ffff", 1, "x"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 3, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773ffff", 2, MIN_VAL), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 3),
            "mykey",
            Arrays.asList("1773ffff", 2, MAX_VAL), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 4, 1803 + 1, 2),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 4, 1803 + 1, 3),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 4, 1803 + 1, 1),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            Arrays.asList("1803jjjj", 1), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 4, 1803 + 1, 3),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            Arrays.asList("1803jjjj", 1), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 4, 1803 + 1, 1),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            Arrays.asList("1803jjjj", 1, "0"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 4, 1803 + 1, 3),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            Arrays.asList("1803jjjj", 1, "A"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 2),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 3),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GT,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 4, 1803 + 1, 3),
            "mykey",
            ImmutableList.of("1773ffff", 2, "2ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "2jjjj"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 1),
            "mykey",
            ImmutableList.of("1773ffff", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 1),
            "mykey",
            ImmutableList.of("1773e", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1368, 3, 1368 + 1, 4),
            "mykey",
            ImmutableList.of("1368qqqqqqqqq", 2, "1qqqq"), ComparisonOperator.GTE,
            ImmutableList.of("1368qqqqqqqqq", 2, "2qqqq"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1368, 3, 1368 + 1, 3),
            "mykey",
            ImmutableList.of("1368qqqqqqqqq", 2, "1qqqq"), ComparisonOperator.GT,
            ImmutableList.of("1368qqqqqqqqq", 2, "2qqqq"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1368, 4, 1368 + 2, 1),
            "mykey",
            ImmutableList.of("1368qqqqqqqqq", 2, "1qqqq"), ComparisonOperator.GT,
            ImmutableList.of("1368qqqqqqqqq", 2, "2qqqq"), ComparisonOperator.LTE);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1368, 3, 1368 + 2, 1),
            "mykey",
            ImmutableList.of("1368qqqqqqqqq", 2, "1qqqq"), ComparisonOperator.GTE,
            ImmutableList.of("1368qqqqqqqqq", 2, "2qqqq"), ComparisonOperator.LTE);

    // mid, nothing
    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1774, 1),
            "mykey",
            ImmutableList.of("1773g", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1774gggg", 2, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1774, 1),
            "mykey",
            ImmutableList.of("1773g", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1774gggg", 2, "1jjjj"), ComparisonOperator.LT);

    // mid, special not exist key
    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 4 + 1),
            "mykey",
            ImmutableList.of("1773ffff", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803x", 1, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773 + 1, 1, 1803 + 1, 1),
            "mykey",
            ImmutableList.of("1773ffff", Integer.MAX_VALUE, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", 1, "1jjjj"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1773, 1, 1803 + 1, 4 + 1),
            "mykey",
            ImmutableList.of("1773ffff", 1, "1ffff"), ComparisonOperator.GTE,
            ImmutableList.of("1803jjjj", Integer.MAX_VALUE, "1jjjj"), ComparisonOperator.LT);

    // one group
    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1998, 1, 1999 + 1, 1),
            "mykey",
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of("1999xxxxxxxxxx", 1, "1999xxxxxxxxxx"), ComparisonOperator.LT);

    // one group + 1 more row
    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1998, 1, 1999 + 1, 2),
            "mykey",
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of("1999xxxxxxxxxx", 1, "2xxxxx"), ComparisonOperator.LT);

    // no upper
    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1998, 1, 2000, 4 + 1),
            "mykey",
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of("2", 1, ""), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1998, 1, 2000, 4 + 1),
            "mykey",
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of("1999xxxxxxxxxx", 2, "3"), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1998, 1, 2000, 4 + 1),
            "mykey",
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.LT);

    assertThat.withSql(sql())
        .checkQueryIteratorBySk(expectedRangeQueryPartially(1998, 1, 2000, 4 + 1),
            "mykey",
            ImmutableList.of("1998wwwwwwwww", 1, "1wwww"), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.NOP);
  }

  public Consumer<Iterator<GenericRecord>> testQueryBySk(List<Integer> resultIdList) {
    return testQueryBySk(resultIdList, true);
  }

  public Consumer<Iterator<GenericRecord>> testQueryBySk(List<Integer> resultIdList, boolean asc) {
    return iterator -> {
      // for debug
//      System.out.println("-------");
//      while (iterator.hasNext()) {
//        GenericRecord record = iterator.next();
//        Object[] values = record.getValues();
//        System.out.println(Arrays.asList(values));
//      }
//      System.out.println("-------");

      if (resultIdList.isEmpty()) {
        assertThat(iterator.hasNext(), is(false));
        return;
      }

      int i = asc ? 0 : resultIdList.size() - 1;
      int count = 0;
      List<Object[]> expected = getAllRows(HrSchema.EMPS);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();

        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        Object[] expectedRow = expected.get(resultIdList.get(i) - 1);
        // System.out.println(resultIdList + " " + Arrays.asList(expectedRow));
        assertThat(values, is(expectedRow));
        count++;
        if (asc) {
          i++;
        } else {
          i--;
        }
      }
      System.out.println(count);
      assertThat(count, is(resultIdList.size()));
      assertThat(iterator.hasNext(), is(false));
    };
  }

}
