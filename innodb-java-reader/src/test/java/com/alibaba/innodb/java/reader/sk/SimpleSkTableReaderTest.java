package com.alibaba.innodb.java.reader.sk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.util.ThreadContext;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.alibaba.innodb.java.reader.Constants.MAX_VAL;
import static com.alibaba.innodb.java.reader.Constants.MIN_VAL;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class SimpleSkTableReaderTest extends AbstractTest {

  String sql = "CREATE TABLE `emp` (\n"
      + "  `id` int(11) NOT NULL,\n"
      + "  `empno` bigint(20) NOT NULL,\n"
      + "  `name` varchar(64) NOT NULL,\n"
      + "  `deptno` int(11) NOT NULL,\n"
      + "  `gender` char(1) NOT NULL,\n"
      + "  `birthdate` date NOT NULL,\n"
      + "  `city` varchar(100) NOT NULL,\n"
      + "  `salary` int(11) NOT NULL,\n"
      + "  `age` int(11) NOT NULL,\n"
      + "  `joindate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
      + "  `level` int(11) NOT NULL,\n"
      + "  `profile` text CHARSET latin1 NOT NULL,\n"
      + "  `address` varchar(500) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"
      + "  `email` varchar(100) DEFAULT NULL,\n"
      + "  PRIMARY KEY (`id`),\n"
      + "  UNIQUE KEY `empno` (`empno`),\n"
      + "  KEY `name` (`name`),\n"
      + "  KEY `idx_city` (`city`),\n"
      + "  KEY `age` (`age`),\n"
      + "  KEY `age_2` (`age`,`salary`),\n"
      + "  KEY `key_join_date` (`joindate`),\n"
      + "  KEY `deptno` (`deptno`,`level`,`name`),\n"
      + "  KEY `deptno_2` (`deptno`,`level`,`empno`),\n"
      + "  KEY `address` (`address`(255)),\n"
      + "  KEY `email` (`email`(3)),\n"
      + "  KEY `key_level` (`level`),\n"
      + "  FULLTEXT KEY `profile` (`profile`),\n"
      + "  CONSTRAINT `emp_ibfk_1` FOREIGN KEY (`deptno`) REFERENCES `dept` (`deptno`)\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

  //==========================================================================
  // queryAllIterator test
  //==========================================================================

  @Test
  public void testQueryAllIteratorMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryAllIterator(testQueryBySk(allRowsPk(), true));

    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryAllIteratorDesc(testQueryBySk(allRowsPk(), false));
  }

  @Test
  public void testQueryAllIteratorMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSql(sql)
        .checkQueryAllIterator(testQueryBySk(allRowsPk(), true));

    assertTestOf(this)
        .withMysql57()
        .withSql(sql)
        .checkQueryAllIteratorDesc(testQueryBySk(allRowsPk(), false));
  }

  @Test
  public void testQueryAllIteratorMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSql(sql)
        .checkQueryAllIterator(testQueryBySk(allRowsPk(), true));

    assertTestOf(this)
        .withMysql80()
        .withSql(sql)
        .checkQueryAllIteratorDesc(testQueryBySk(allRowsPk(), false));
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: name
  //==========================================================================

  @Test
  public void testQueryBySkNameMysql56() {
    testQueryBySkName(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkNameMysql57() {
    testQueryBySkName(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkNameMysql80() {
    testQueryBySkName(a -> a.withMysql80());
  }

  public void testQueryBySkName(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select * from emp FORCE INDEX (name) where name = 'Yue';
    // 1 row
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(19)),
            "name",
            ImmutableList.of("Yue"), ComparisonOperator.GTE,
            ImmutableList.of("Yue"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name = 'No';
    // empty result
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of("NO"), ComparisonOperator.GTE,
            ImmutableList.of("NO"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name = 'ZZZ';
    // empty result
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of("ZZZ"), ComparisonOperator.GTE,
            ImmutableList.of("ZZZ"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name > '' and name <= '';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of(""), ComparisonOperator.GTE,
            ImmutableList.of(""), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name <= '';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of(), ComparisonOperator.GTE,
            ImmutableList.of(""), ComparisonOperator.LTE
        );

    // ~~~ test query case sensitive

    // select * from emp FORCE INDEX (name) where name = 'sMiTH' and name = 'Smith';
    // 2 rows (case insensitive)
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(5, 15)),
            "name",
            ImmutableList.of("sMiTH"), ComparisonOperator.GTE,
            ImmutableList.of("Smith"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name = 'SMITH';
    // 2 rows
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(5, 15)),
            "name",
            ImmutableList.of("SMITH"), ComparisonOperator.GTE,
            ImmutableList.of("SMITH"), ComparisonOperator.LTE
        );

    // ~~~ test all rows

    // select * from emp FORCE INDEX (name) where name >= '';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(allRowsPkOrderBy("name")),
            "name",
            ImmutableList.of(""), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.LTE
        );

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(allRowsPkOrderBy("name")),
            "name",
            ImmutableList.of("A"), ComparisonOperator.GTE,
            ImmutableList.of("z"), ComparisonOperator.LTE
        );

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(allRowsPkOrderBy("name")),
            "name",
            ImmutableList.of("a"), ComparisonOperator.GTE,
            ImmutableList.of("Z"), ComparisonOperator.LTE
        );

    // ~~~ test partial result

    Predicate<Object[]> predicate = o ->
        ((String) o[2]).compareToIgnoreCase("A") >= 0
            && ((String) o[2]).compareToIgnoreCase("M") <= 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "name")),
            "name",
            ImmutableList.of("A"), ComparisonOperator.GTE,
            ImmutableList.of("M"), ComparisonOperator.LTE
        );

    // ~~~ test partial result

    // select * from emp FORCE INDEX (name) where name >= 'Jane' and name < 'Sarah';
    predicate = o ->
        ((String) o[2]).compareToIgnoreCase("Jane") >= 0
            && ((String) o[2]).compareToIgnoreCase("Sarah") < 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "name")),
            "name",
            ImmutableList.of("Jane"), ComparisonOperator.GTE,
            ImmutableList.of("Sarah"), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (name) where name > 'Jane' and name < 'Sarah';
    predicate = o ->
        ((String) o[2]).compareToIgnoreCase("Jane") > 0
            && ((String) o[2]).compareToIgnoreCase("Sarah") < 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "name")),
            "name",
            ImmutableList.of("Jane"), ComparisonOperator.GT,
            ImmutableList.of("Sarah"), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (name) where name > 'Jane' and name <= 'Sarah';
    predicate = o ->
        ((String) o[2]).compareToIgnoreCase("Jane") > 0
            && ((String) o[2]).compareToIgnoreCase("Sarah") <= 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "name")),
            "name",
            ImmutableList.of("Jane"), ComparisonOperator.GT,
            ImmutableList.of("Sarah"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name >= 'Jane' and name <= 'Sarah';
    predicate = o ->
        ((String) o[2]).compareToIgnoreCase("Jane") >= 0
            && ((String) o[2]).compareToIgnoreCase("Sarah") <= 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "name")),
            "name",
            ImmutableList.of("Jane"), ComparisonOperator.GTE,
            ImmutableList.of("Sarah"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name >= 'Janey' and name <= 'Sarah';
    predicate = o ->
        ((String) o[2]).compareToIgnoreCase("Janey") >= 0
            && ((String) o[2]).compareToIgnoreCase("Sarah") <= 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "name")),
            "name",
            ImmutableList.of("Janey"), ComparisonOperator.GTE,
            ImmutableList.of("Sarah"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name > 'John' and name < 'OSCAz';
    predicate = o ->
        ((String) o[2]).compareToIgnoreCase("John") > 0
            && ((String) o[2]).compareToIgnoreCase("OSCAz") < 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "name")),
            "name",
            ImmutableList.of("John"), ComparisonOperator.GT,
            ImmutableList.of("OSCAz"), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (name) where name > 'Scott' and name < 'smithy';
    predicate = o ->
        ((String) o[2]).compareToIgnoreCase("Scott") > 0
            && ((String) o[2]).compareToIgnoreCase("smithy") < 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "name")),
            "name",
            ImmutableList.of("Scott"), ComparisonOperator.GT,
            ImmutableList.of("smithy"), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (name) where name > 'Scott' and name <= 'smit';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of("Scott"), ComparisonOperator.GT,
            ImmutableList.of("smit"), ComparisonOperator.LTE
        );

    // ~~~ test left open

    // select * from emp FORCE INDEX (name) where name <= 'Eric';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(Arrays.asList(14, 1)),
            "name",
            ImmutableList.of(MIN_VAL), ComparisonOperator.GTE,
            ImmutableList.of("Eric"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name < 'Eric';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(Arrays.asList(14)),
            "name",
            ImmutableList.of(), ComparisonOperator.GTE,
            ImmutableList.of("Eric"), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (name) where name <= 'adams';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(Arrays.asList(14)),
            "name",
            ImmutableList.of(""), ComparisonOperator.GT,
            ImmutableList.of("adams"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name < 'ADAMS';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of(""), ComparisonOperator.GT,
            ImmutableList.of("ADAMS"), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (name) where name <= 'ADAMS' and name > 'ZZZ';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of(MAX_VAL), ComparisonOperator.GT,
            ImmutableList.of("ADAMS"), ComparisonOperator.LTE
        );

    // ~~~ test right close

    // select * from emp FORCE INDEX (name) where name >= 'SMITH';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(Arrays.asList(5, 15, 19)),
            "name",
            ImmutableList.of("SMITH"), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name > 'SMITH';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(Arrays.asList(19)),
            "name",
            ImmutableList.of("SMITH"), ComparisonOperator.GT,
            ImmutableList.of(MAX_VAL), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (name) where name >= 'YUE';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(Arrays.asList(19)),
            "name",
            ImmutableList.of("YUE"), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name > 'YUE';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of("YUE"), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name > 'YUE';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of("YUE"), ComparisonOperator.GT,
            ImmutableList.of(MAX_VAL), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (name) where name >= 'YUE' and name < '0';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of("YUE"), ComparisonOperator.GTE,
            ImmutableList.of(MIN_VAL), ComparisonOperator.LTE
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: city
  //==========================================================================

  @Test
  public void testQueryBySkCityMysql56() {
    testQueryBySkCity(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkCityMysql57() {
    testQueryBySkCity(a -> a.withMysql80());
  }

  @Test
  public void testQueryBySkCityMysql80() {
    testQueryBySkCity(a -> a.withMysql80());
  }

  public void testQueryBySkCity(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select * from emp FORCE INDEX (idx_city) where city > 'A' and city < 'Z';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(allRowsPkOrderBy("city")),
            "idx_city",
            ImmutableList.of("A"), ComparisonOperator.GT,
            ImmutableList.of("z"), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (idx_city) where city <= 'London';
    Predicate<Object[]> predicate = o ->
        ((String) o[6]).compareToIgnoreCase("") > 0
            && ((String) o[6]).compareToIgnoreCase("London") <= 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "city")),
            "idx_city",
            ImmutableList.of(""), ComparisonOperator.GT,
            ImmutableList.of("London"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (idx_city) where city > 'Seattle';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(5, 17)),
            "idx_city",
            ImmutableList.of("Seattle"), ComparisonOperator.GT,
            ImmutableList.of(MAX_VAL), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (idx_city) where city = 'New York';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(1, 6, 8, 9, 11, 18, 19)),
            "idx_city",
            ImmutableList.of("New York"), ComparisonOperator.GTE,
            ImmutableList.of("New York"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (idx_city) where city = 'berlin';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(2, 12, 16)),
            "idx_city",
            ImmutableList.of("berlin"), ComparisonOperator.GTE,
            ImmutableList.of("berlin"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (idx_city) where city = 'beijing';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(4, 13, 15)),
            "idx_city",
            ImmutableList.of("beijing"), ComparisonOperator.GTE,
            ImmutableList.of("beijing"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (idx_city) where city = 'LA';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(3, 7, 10, 14, 20)),
            "idx_city",
            ImmutableList.of("LA"), ComparisonOperator.GTE,
            ImmutableList.of("LA"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (idx_city) where city = 'tokyo';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(5, 17)),
            "idx_city",
            ImmutableList.of("tokyo"), ComparisonOperator.GTE,
            ImmutableList.of("tokyo"), ComparisonOperator.LTE
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: age
  //==========================================================================

  @Test
  public void testQueryBySkAgeMysql56() {
    testQueryBySkAge(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkAgeMysql57() {
    testQueryBySkAge(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkAgeMysql80() {
    testQueryBySkAge(a -> a.withMysql80());
  }

  public void testQueryBySkAge(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select * from emp FORCE INDEX (age) where age > 0 and age < 100;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(allRowsPkOrderBy("age")),
            "age",
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (age) where age >=20 and age <= 60;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(allRowsPkOrderBy("age")),
            "age",
            ImmutableList.of(20), ComparisonOperator.GTE,
            ImmutableList.of(60), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (age) where age > 26 and age <= 38;
    Predicate<Object[]> predicate = o ->
        ((int) o[8]) > 26
            && ((int) o[8]) <= 38;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "age")),
            "age",
            ImmutableList.of(26), ComparisonOperator.GT,
            ImmutableList.of(38), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (age) where age > 40;
    predicate = o -> ((int) o[8]) > 40;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "age")),
            "age",
            ImmutableList.of(40), ComparisonOperator.GT,
            ImmutableList.of(MAX_VAL), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (age) where age < 40;
    predicate = o -> ((int) o[8]) < 40;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(someRowsOrderBy(predicate, "age")),
            "age",
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(40), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (age) where age > 100;
    predicate = o -> ((int) o[8]) > 100;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(
            testQueryBySk(ImmutableList.of()),
            "age",
            ImmutableList.of(100), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: joindate
  //==========================================================================

  @Test
  public void testQueryBySkJoinDateMysql56() {
    testQueryBySkJoinDate(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkJoinDateMysql57() {
    testQueryBySkJoinDate(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkJoinDateMysql80() {
    testQueryBySkJoinDate(a -> a.withMysql80());
  }

  public void testQueryBySkJoinDate(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select * from emp FORCE INDEX (key_join_date) where joindate > '2017-05-01 00:00:00';
    Predicate<Object[]> predicate = o ->
        ((String) o[9]).compareToIgnoreCase("2017-05-01 00:00:00") > 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(someRowsOrderBy(predicate, "joindate")),
            "key_join_date",
            ImmutableList.of("2017-05-01 00:00:00"), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (key_join_date) where joindate > '2010-05-01 00:00:00';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(allRowsPkOrderBy("joindate")),
            "key_join_date",
            ImmutableList.of("2010-05-01 00:00:00"), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (key_join_date) where joindate > '2018-04-09 08:59:00'
    // and joindate < '2019-12-31 00:15:31';
    predicate = o ->
        ((String) o[9]).compareToIgnoreCase("2018-04-09 08:59:00") > 0
            && ((String) o[9]).compareToIgnoreCase("2019-12-31 00:15:31") < 0;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(someRowsOrderBy(predicate, "joindate")),
            "key_join_date",
            ImmutableList.of("2018-04-09 08:59:00"), ComparisonOperator.GT,
            ImmutableList.of("2019-12-31 00:15:31"), ComparisonOperator.LT
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: age & salary
  //==========================================================================

  @Test
  public void testQueryBySkAgeSalaryMysql56() {
    testQueryBySkAgeSalary(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkAgeSalaryMysql57() {
    testQueryBySkAgeSalary(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkAgeSalaryMysql80() {
    testQueryBySkAgeSalary(a -> a.withMysql80());
  }

  public void testQueryBySkAgeSalary(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select * from emp FORCE INDEX (age_2) where age > 10 and salary > 1000;
    Predicate<Object[]> predicate1 = o -> ((int) o[8]) > 10;
    Predicate<Object[]> predicate2 = o -> ((int) o[7]) > 1000;
    Predicate<Object[]> predicate = predicate1.and(predicate2);
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(someRowsOrderBy2(predicate, "age", "salary")),
            "age_2",
            ImmutableList.of(20, 0), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (age_2) where age <= 27;
    predicate1 = o -> ((int) o[8]) <= 27;
    predicate = predicate1;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(someRowsOrderBy2(predicate, "age", "salary")),
            "age_2",
            ImmutableList.of(), ComparisonOperator.GTE,
            ImmutableList.of(27), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (age_2) where age > 27;
    predicate1 = o -> ((int) o[8]) > 27;
    predicate = predicate1;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(someRowsOrderBy2(predicate, "age", "salary")),
            "age_2",
            ImmutableList.of(27), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (age_2) where age >= 27;
    predicate1 = o -> ((int) o[8]) >= 27;
    predicate = predicate1;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(someRowsOrderBy2(predicate, "age", "salary")),
            "age_2",
            ImmutableList.of(27), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select * from emp FORCE INDEX (age_2) where age >= 27 and age < 42;
    predicate1 = o -> ((int) o[8]) >= 27;
    predicate2 = o -> ((int) o[8]) < 42;
    predicate = predicate1.and(predicate2);
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(someRowsOrderBy2(predicate, "age", "salary")),
            "age_2",
            ImmutableList.of(27), ComparisonOperator.GTE,
            ImmutableList.of(42), ComparisonOperator.LT
        );

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(1, 16, 2, 14, 15, 19, 9)),
            "age_2",
            ImmutableList.of(30, 45000), ComparisonOperator.GTE,
            ImmutableList.of(41), ComparisonOperator.LT
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: deptno level name
  //==========================================================================

  @Test
  public void testQueryBySkDeptnoLevelNameMysql56() {
    testQueryBySkDeptnoLevelName(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkDeptnoLevelNameMysql57() {
    testQueryBySkDeptnoLevelName(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkDeptnoLevelNameMysql80() {
    testQueryBySkDeptnoLevelName(a -> a.withMysql80());
  }

  public void testQueryBySkDeptnoLevelName(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select * from emp FORCE INDEX (deptno) where deptno = 20;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(7, 3, 14, 10, 18, 1, 13, 20, 16)),
            "deptno",
            ImmutableList.of(20), ComparisonOperator.GTE,
            ImmutableList.of(20), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (deptno) where deptno = 30;
    Predicate<Object[]> predicate1 = o -> ((int) o[3]) == 30;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(
            someRowsOrderBy3(predicate1, "deptno", "level", "name")),
            "deptno",
            ImmutableList.of(30), ComparisonOperator.GTE,
            ImmutableList.of(30), ComparisonOperator.LTE
        );

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(14, 10, 18, 1, 13, 20, 16)),
            "deptno",
            ImmutableList.of(20, 5), ComparisonOperator.GTE,
            ImmutableList.of(20, 7, "ZZZ"), ComparisonOperator.LT
        );

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(14, 10, 18, 1, 13, 20, 16)),
            "deptno",
            ImmutableList.of(20, 5, "AA"), ComparisonOperator.GT,
            ImmutableList.of(20, 7, "Scott"), ComparisonOperator.LTE
        );

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(10, 18, 1, 13, 20, 16)),
            "deptno",
            ImmutableList.of(20, 5, "AZ"), ComparisonOperator.GT,
            ImmutableList.of(20, 7, "Scott"), ComparisonOperator.LTE
        );

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(14, 10, 18, 1, 13, 20)),
            "deptno",
            ImmutableList.of(20, 5), ComparisonOperator.GTE,
            ImmutableList.of(20, 7, "Scott"), ComparisonOperator.LT
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: address, utf8_bin
  //==========================================================================

  @Test
  public void testQueryBySkAddress56() {
    testQueryBySkAddress(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkAddress57() {
    testQueryBySkAddress(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkAddress80() {
    testQueryBySkAddress(a -> a.withMysql80());
  }

  public void testQueryBySkAddress(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select * from emp FORCE INDEX (address) where address > 'a' and address < 'z';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(2)),
            "address",
            ImmutableList.of("a"), ComparisonOperator.GTE,
            ImmutableList.of("z"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (address) where address > 'a' and address < 'Z';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(ImmutableList.of()),
            "address",
            ImmutableList.of("a"), ComparisonOperator.GTE,
            ImmutableList.of("Z"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (address) where address > 'A' and address < 'z';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(14, 18, 2)),
            "address",
            ImmutableList.of("A"), ComparisonOperator.GTE,
            ImmutableList.of("z"), ComparisonOperator.LTE
        );

    // select * from emp FORCE INDEX (address) where address > 'main street';
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(2)),
            "address",
            ImmutableList.of("main street"), ComparisonOperator.GTE,
            ImmutableList.of("main street"), ComparisonOperator.LTE
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: age, set sk ordinal
  //
  // mysql> SELECT * FROM INFORMATION_SCHEMA.INNODB_SYS_INDEXES WHERE TABLE_ID = 3480;
  //+----------+------------------+----------+------+----------+---------+-------+
  //| INDEX_ID | NAME             | TABLE_ID | TYPE | N_FIELDS | PAGE_NO | SPACE |
  //+----------+------------------+----------+------+----------+---------+-------+
  //|     6132 | PRIMARY          |     3480 |    3 |        1 |       3 |  3466 |
  //|     6138 | FTS_DOC_ID_INDEX |     3480 |    2 |        1 |       4 |  3466 |
  //|     6139 | empno            |     3480 |    2 |        1 |       5 |  3466 |
  //|     6140 | name             |     3480 |    0 |        1 |       6 |  3466 |
  //|     6141 | idx_city         |     3480 |    0 |        1 |       7 |  3466 |
  //|     6142 | age              |     3480 |    0 |        1 |       8 |  3466 |
  //|     6143 | age_2            |     3480 |    0 |        2 |       9 |  3466 |
  //|     6144 | key_join_date    |     3480 |    0 |        1 |      10 |  3466 |
  //|     6145 | deptno           |     3480 |    0 |        3 |      11 |  3466 |
  //|     6146 | deptno_2         |     3480 |    0 |        3 |      12 |  3466 |
  //|     6147 | address          |     3480 |    0 |        1 |      13 |  3466 |
  //|     6148 | profile          |     3480 |   32 |        1 |      -1 |  3466 |
  //|     6156 | key_level        |     3480 |    0 |        1 |      15 |  3466 |
  //+----------+------------------+----------+------+----------+---------+-------+
  //==========================================================================

  @Test(expected = IllegalArgumentException.class)
  public void testQueryBySkLevel56NegateOutOfRangeOrdinal() {
    ThreadContext.putSkOrdinal(32);

    assertTestOf(this)
        .withSql(sql)
        .withMysql56()
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(19)),
            "name",
            ImmutableList.of("Yue"), ComparisonOperator.GTE,
            ImmutableList.of("Yue"), ComparisonOperator.LTE
        );
  }

  /**
   * Caused by: com.alibaba.innodb.java.reader.exception.ReaderException:
   * Failed to make type compatible for key: [Yue]
   */
  @Test(expected = ReaderException.class)
  public void testQueryBySkLevel56NegateWrongOrdinalClassNotCast() {
    ThreadContext.putSkOrdinal(4);

    assertTestOf(this)
        .withSql(sql)
        .withMysql56()
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(19)),
            "name",
            ImmutableList.of("Yue"), ComparisonOperator.GTE,
            ImmutableList.of("Yue"), ComparisonOperator.LTE
        );
  }

  /**
   * Should check age, but goes to address sk, still got result, but nothing matches
   */
  @Test
  public void testQueryBySkLevel56NegateWrongOrdinal() {
    ThreadContext.putSkOrdinal(8);

    assertTestOf(this)
        .withSql(sql)
        .withMysql56()
        .checkQueryIteratorBySk(testQueryBySk(ImmutableList.of()),
            "name",
            ImmutableList.of("Yue"), ComparisonOperator.GTE,
            ImmutableList.of("Yue"), ComparisonOperator.LTE
        );
  }

  /**
   * Secondary key type does not support FULLTEXT_KEY
   */
  @Test(expected = IllegalStateException.class)
  public void testQueryBySkLevel56NegateWrongOrdinal2() {
    ThreadContext.putSkOrdinal(11);

    assertTestOf(this)
        .withSql(sql)
        .withMysql56()
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(19)),
            "name",
            ImmutableList.of("Yue"), ComparisonOperator.GTE,
            ImmutableList.of("Yue"), ComparisonOperator.LTE
        );
  }

  @Test
  public void testQueryBySkPutOrdinal56() {
    testQueryBySkPutOrdinal(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkPutOrdinal57() {
    testQueryBySkPutOrdinal(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkPutOrdinal80() {
    testQueryBySkPutOrdinal(a -> a.withMysql80());
  }

  public void testQueryBySkPutOrdinal(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    ThreadContext.putSkOrdinal(1);

    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(19)),
            "name",
            ImmutableList.of("Yue"), ComparisonOperator.GTE,
            ImmutableList.of("Yue"), ComparisonOperator.LTE
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: level, set sk root page number
  //==========================================================================

  @Test(expected = ReaderException.class)
  public void testQueryBySkLevel56NegateWrongPage() {
    assertTestOf(this)
        .withSql(sql)
        .withMysql56()
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(2)),
            "key_level",
            ImmutableList.of(6), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.NOP
        );
  }

  /**
   * Should check level, but goes to pk, same type.
   * <p>
   * Exception: Cannot query pk record from sk record
   */
  @Test(expected = ReaderException.class)
  public void testQueryBySkLevel56NegateWrongPage2() {
    ThreadContext.putSkRootPageNumber(3L);

    assertTestOf(this)
        .withSql(sql)
        .withMysql56()
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(2)),
            "key_level",
            ImmutableList.of(6), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.NOP
        );
  }

  /**
   * Should check level, but goes to age sk, still got result, but nothing matches
   */
  @Test
  public void testQueryBySkLevel56NegateWrongPage3() {
    ThreadContext.putSkRootPageNumber(8L);

    assertTestOf(this)
        .withSql(sql)
        .withMysql56()
        .checkQueryIteratorBySk(testQueryBySk(ImmutableList.of()),
            "key_level",
            ImmutableList.of(6), ComparisonOperator.GT,
            ImmutableList.of(10), ComparisonOperator.LTE
        );
  }

  @Test
  public void testQueryBySkLevel56() {
    testQueryBySkLevel(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkLevel57() {
    testQueryBySkLevel(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkLevel80() {
    testQueryBySkLevel(a -> a.withMysql80());
  }

  public void testQueryBySkLevel(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    if (isMysql8Flag.get()) {
      ThreadContext.putSkRootPageNumber(17L);
    } else {
      ThreadContext.putSkRootPageNumber(16L);
    }

    // select * from emp FORCE INDEX (key_level) where level > 6;
    assertThat.withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(9, 11, 16, 19, 2, 15, 12, 17, 4)),
            "key_level",
            ImmutableList.of(6), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.NOP
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: age, order: desc
  //==========================================================================

  @Test
  public void testQueryBySkAgeDescMysql56() {
    testQueryBySkAgeDesc(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkAgeDescMysql57() {
    testQueryBySkAgeDesc(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkAgeDescMysql80() {
    testQueryBySkAgeDesc(a -> a.withMysql80());
  }

  public void testQueryBySkAgeDesc(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select * from emp FORCE INDEX (age) where age > 0 and age < 100 order by age desc;
    assertThat.withSql(sql)
        .checkQueryIteratorBySkDesc(testQueryBySk(
            allRowsPkOrderBy("age"), false),
            "age",
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: age, projection
  //==========================================================================

  @Test
  public void testQueryBySkAgeProjectionMysql56() {
    testQueryBySkAgeProjection(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkAgeProjectionMysql57() {
    testQueryBySkAgeProjection(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkAgeProjectionMysql80() {
    testQueryBySkAgeProjection(a -> a.withMysql80());
  }

  public void testQueryBySkAgeProjection(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    List<String> projection = ImmutableList.of("empno", "age", "gender", "name");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            allRowsPkOrderBy("age"), projection, true),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of("empno", "joindate");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            allRowsPkOrderBy("age"), projection, true),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of("profile", "address");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            allRowsPkOrderBy("age"), projection, true),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of("birthdate", "age", "level", "city");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            allRowsPkOrderBy("age"), projection, true),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of("city");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            allRowsPkOrderBy("age"), projection, true),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of();
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            allRowsPkOrderBy("age"), projection, true),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    // covering index
    projection = ImmutableList.of("age");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            allRowsPkOrderBy("age"), projection, true),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: age, projection, order by
  //==========================================================================

  @Test
  public void testQueryBySkAgeProjectionDescMysql56() {
    testQueryBySkAgeProjectionDesc(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkAgeProjectionDescMysql57() {
    testQueryBySkAgeProjectionDesc(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkAgeProjectionDescMysql80() {
    testQueryBySkAgeProjectionDesc(a -> a.withMysql80());
  }

  public void testQueryBySkAgeProjectionDesc(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    List<String> projection = ImmutableList.of("empno", "age", "gender", "name");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjectionDesc(testQueryBySk(
            allRowsPkOrderBy("age"), projection, false),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of("empno", "joindate");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjectionDesc(testQueryBySk(
            allRowsPkOrderBy("age"), projection, false),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of("profile", "address");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjectionDesc(testQueryBySk(
            allRowsPkOrderBy("age"), projection, false),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of("gender", "salary", "level", "city");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjectionDesc(testQueryBySk(
            allRowsPkOrderBy("age"), projection, false),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of("deptno");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjectionDesc(testQueryBySk(
            allRowsPkOrderBy("age"), projection, false),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );

    projection = ImmutableList.of();
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjectionDesc(testQueryBySk(
            allRowsPkOrderBy("age"), projection, false),
            "age", projection,
            ImmutableList.of(0), ComparisonOperator.GT,
            ImmutableList.of(100), ComparisonOperator.LT
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: address, projection
  //==========================================================================

  @Test
  public void testQueryBySkAddressProjectionMysql56() {
    testQueryBySkAddressProjection(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkAddressProjectionMysql57() {
    testQueryBySkAddressProjection(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkAddressProjectionMysql80() {
    testQueryBySkAddressProjection(a -> a.withMysql80());
  }

  public void testQueryBySkAddressProjection(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // select address from emp force index (`address`) order by address;
    List<String> projection = ImmutableList.of("address");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 19, 20, 14, 18, 2, 13),
            projection, true),
            "address", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select empno, address from emp force index (`address`) order by address;
    projection = ImmutableList.of("empno", "address");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 19, 20, 14, 18, 2, 13),
            projection, true),
            "address", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select empno, name, address from emp force index (`address`) order by address;
    projection = ImmutableList.of("empno", "name", "address");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 19, 20, 14, 18, 2, 13),
            projection, true),
            "address", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select id from emp force index (`address`) where address > 'a';
    projection = ImmutableList.of("id");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(2, 13),
            projection, true),
            "address", projection,
            ImmutableList.of("a"), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // select empno, address from emp force index (`address`) where address > 'o';
    projection = ImmutableList.of("address", "empno");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(13),
            projection, true),
            "address", projection,
            ImmutableList.of("o"), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    projection = ImmutableList.of("address");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(13),
            projection, true),
            "address", projection,
            ImmutableList.of("老北京胡同Z区"), ComparisonOperator.GTE,
            ImmutableList.of("老北京胡同Z区"), ComparisonOperator.LTE
        );

    projection = ImmutableList.of("address");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(2),
            projection, true),
            "address", projection,
            ImmutableList.of("main street"), ComparisonOperator.GTE,
            ImmutableList.of("main street"), ComparisonOperator.LTE
        );

    projection = ImmutableList.of("address");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            ImmutableList.of(),
            projection, true),
            "address", projection,
            ImmutableList.of("main street"), ComparisonOperator.GT,
            ImmutableList.of("main street"), ComparisonOperator.LT
        );

    // TODO mysql will not include NULL addresses but result has
    // select empno, address from emp force index (`address`) where address < 'o';
    projection = ImmutableList.of("address", "empno");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 19, 20, 14, 18, 2),
            projection, true),
            "address", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of("o"), ComparisonOperator.LT
        );
  }

  //==========================================================================
  // getRecordIteratorBySk test, single key: email, projection
  //==========================================================================

  @Test
  public void testQueryBySkEmailProjectionMysql56() {
    testQueryBySkEmailProjection(a -> a.withMysql56());
  }

  @Test
  public void testQueryBySkEmailProjectionMysql57() {
    testQueryBySkEmailProjection(a -> a.withMysql57());
  }

  @Test
  public void testQueryBySkEmailProjectionMysql80() {
    testQueryBySkEmailProjection(a -> a.withMysql80());
  }

  /**
   * <pre>
   * +----+-------+------------------+
   * | id | empno | email            |
   * +----+-------+------------------+
   * | 14 |   115 | adams@test.com   |
   * |  1 |   100 | eric@test.com    |
   * |  7 |   108 | james@test.com   |
   * | 10 |   111 | jane@test.com    |
   * |  8 |   109 | john@test.com    |
   * |  4 |   105 | json@test.com    |
   * | 18 |   122 | kidd@test.com    |
   * | 13 |   114 | lara@test.com    |
   * |  6 |   107 | lucy@test.com    |
   * | 17 |   121 | martin@test.com  |
   * |  9 |   110 | miller@test.com  |
   * |  2 |   101 | neo@test.com     |
   * | 20 |   124 | oscar@test.com   |
   * | 12 |   113 | paul@test.com    |
   * | 11 |   112 | sarah02@test.com |
   * |  3 |   102 | sarah@test.com   |
   * | 16 |   120 | scott@test.com   |
   * | 15 |   116 | smith02@test.com |
   * |  5 |   106 | smith@test.com   |
   * | 19 |   123 | yue@test.com     |
   * +----+-------+------------------+
   * </pre>
   */
  public void testQueryBySkEmailProjection(Consumer<AssertThat> func) {
    AssertThat assertThat = assertTestOf(this);
    func.accept(assertThat);

    // TODO mysql will take smith02@test.com ahead of smith@test.com with order by
    // but actual physical record is not, this is ok, we do not guarantee order
    // select empno, email from emp force index (email) order by email;
    List<String> projection = ImmutableList.of("empno", "email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(14, 1, 7, 10, 8, 4, 18, 13, 6, 17, 9, 2, 20, 12, 3, 11, 16, 5, 15, 19),
            projection, true),
            "email", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    // although key var len is 3, we still look up to primary key to get record,
    // covering index should not be enabled.
    projection = ImmutableList.of("email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(14, 1, 7, 10, 8, 4, 18, 13, 6, 17, 9, 2, 20, 12, 3, 11, 16, 5, 15, 19),
            projection, true),
            "email", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    projection = ImmutableList.of("empno", "email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(2, 20, 12, 3, 11, 16, 5, 15, 19),
            projection, true),
            "email", projection,
            ImmutableList.of("mim"), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    projection = ImmutableList.of("email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(14, 1, 7, 10, 8, 4),
            projection, true),
            "email", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of("kid"), ComparisonOperator.LT
        );

    projection = ImmutableList.of("empno", "email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(13, 6, 17, 9, 2, 20, 12, 3, 11),
            projection, true),
            "email", projection,
            ImmutableList.of("kid"), ComparisonOperator.GT,
            ImmutableList.of("sco"), ComparisonOperator.LT
        );

    projection = ImmutableList.of("email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(13, 6, 17, 9, 2, 20, 12, 3, 11),
            projection, true),
            "email", projection,
            ImmutableList.of("kid"), ComparisonOperator.GT,
            ImmutableList.of("sco"), ComparisonOperator.LT
        );

    projection = ImmutableList.of("email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(12, 3, 11, 16, 5, 15, 19),
            projection, true),
            "email", projection,
            ImmutableList.of("p"), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.LT
        );

    projection = ImmutableList.of("email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(12, 3, 11, 16, 5, 15, 19),
            projection, true),
            "email", projection,
            ImmutableList.of("p"), ComparisonOperator.GT,
            ImmutableList.of(), ComparisonOperator.LT
        );

    projection = ImmutableList.of("email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(14, 1, 7, 10, 8, 4),
            projection, true),
            "email", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of("k"), ComparisonOperator.LT
        );

    projection = ImmutableList.of("email");
    assertThat.withSql(sql)
        .checkQueryIteratorBySkProjection(testQueryBySk(
            Arrays.asList(14, 1, 7, 10, 8, 4, 18),
            projection, true),
            "email", projection,
            ImmutableList.of(), ComparisonOperator.GT,
            ImmutableList.of("kie"), ComparisonOperator.LTE
        );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testKeyVarLenOutOfRange() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(ImmutableList.of()),
            "email",
            ImmutableList.of("long_email"), ComparisonOperator.GTE,
            ImmutableList.of(), ComparisonOperator.LTE
        );
  }

  //==========================================================================
  // negate test
  //==========================================================================

  @Test(expected = ReaderException.class)
  public void testQueryByNotExistSk() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(2, 12, 16)),
            "not_exist",
            ImmutableList.of("berlin"), ComparisonOperator.GTE,
            ImmutableList.of("berlin"), ComparisonOperator.LTE
        );
  }

  @Test(expected = ReaderException.class)
  public void testQueryByEmptySk() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(2, 12, 16)),
            "",
            ImmutableList.of("berlin"), ComparisonOperator.GTE,
            ImmutableList.of("berlin"), ComparisonOperator.LTE
        );
  }

  /**
   * Caused by: com.alibaba.innodb.java.reader.exception.ReaderException:
   * Failed to make type compatible for key: [a]
   */
  @Test(expected = ReaderException.class)
  public void testQueryBySkWrongType() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryIteratorBySk(testQueryBySk(Arrays.asList(2, 12, 16)),
            "age",
            ImmutableList.of("a"), ComparisonOperator.GTE,
            ImmutableList.of(6), ComparisonOperator.LTE
        );
  }

  public Consumer<Iterator<GenericRecord>> testQueryBySk(List<Integer> resultIdList) {
    return testQueryBySk(resultIdList, ImmutableList.of(), true);
  }

  public Consumer<Iterator<GenericRecord>> testQueryBySk(List<Integer> resultIdList, boolean asc) {
    return testQueryBySk(resultIdList, ImmutableList.of(), asc);
  }

  public Consumer<Iterator<GenericRecord>> testQueryBySk(List<Integer> resultIdList,
                                                         List<String> projection,
                                                         boolean asc) {
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
        // System.out.println(Arrays.asList(values));
        Object[] expectedRow = expected.get(resultIdList.get(i) - 1);
        // System.out.println(resultIdList + " " + Arrays.asList(expectedRow));
        if (projection.isEmpty()) {
          assertThat(values, is(expectedRow));
        } else {
          for (String s : projection) {
            int ordinal = record.getTableDef().getField(s).getOrdinal();
            Object expect = expectedRow[ordinal];
            assertThat(record.get(s), is(expect));
          }
        }
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

  private static List<Integer> allRowsPk() {
    List<Object[]> allRows = getAllRows(HrSchema.EMPS);
    return allRows.stream().map(a -> (Integer) a[0]).collect(Collectors.toList());
  }

  private static List<Integer> allRowsPkOrderBy(String fieldName) {
    List<Object[]> allRows = new ArrayList<>(getAllRows(HrSchema.EMPS));
    int fieldOrdinal = getFieldOrdinal(Employee.class, fieldName);
    sortIgnoreCase(allRows, fieldOrdinal);
//    for (Object[] allRow : allRows) {
//      System.out.println(Arrays.toString(allRow) + " ===");
//    }
    return allRows.stream().map(a -> (Integer) a[0]).collect(Collectors.toList());
  }

  private static List<Integer> someRowsOrderBy(Predicate<Object[]> predicate, String fieldName) {
    List<Object[]> rows = new ArrayList<>(getAllRows(HrSchema.EMPS));
    List<Object[]> filteredRows = rows.stream().filter(predicate::test)
        .collect(Collectors.toList());
    int fieldOrdinal = getFieldOrdinal(Employee.class, fieldName);
    sortIgnoreCase(filteredRows, fieldOrdinal);
    return filteredRows.stream().map(a -> (Integer) a[0]).collect(Collectors.toList());
  }

  private static List<Integer> someRowsOrderBy2(Predicate<Object[]> predicate,
                                                String f1, String f2) {
    List<Object[]> rows = new ArrayList<>(getAllRows(HrSchema.EMPS));
    List<Object[]> filteredRows = rows.stream().filter(predicate::test)
        .collect(Collectors.toList());
    int fieldOrdinal1 = getFieldOrdinal(Employee.class, f1);
    int fieldOrdinal2 = getFieldOrdinal(Employee.class, f2);
    sortIgnoreCase(filteredRows, fieldOrdinal1, fieldOrdinal2);
    return filteredRows.stream().map(a -> (Integer) a[0]).collect(Collectors.toList());
  }

  private static List<Integer> someRowsOrderBy3(Predicate<Object[]> predicate,
                                                String f1, String f2, String f3) {
    List<Object[]> rows = new ArrayList<>(getAllRows(HrSchema.EMPS));
    List<Object[]> filteredRows = rows.stream().filter(predicate::test)
        .collect(Collectors.toList());
    int fieldOrdinal1 = getFieldOrdinal(Employee.class, f1);
    int fieldOrdinal2 = getFieldOrdinal(Employee.class, f2);
    int fieldOrdinal3 = getFieldOrdinal(Employee.class, f3);
    sortIgnoreCase(filteredRows, fieldOrdinal1, fieldOrdinal2, fieldOrdinal3);
    return filteredRows.stream().map(a -> (Integer) a[0]).collect(Collectors.toList());
  }

  private static void sort(List<Object[]> allRows, int... fieldOrdinal) {
    Collections.sort(allRows, new Comparator<Object[]>() {
      @Override
      public int compare(Object[] o1, Object[] o2) {
        for (int i : fieldOrdinal) {
          Comparable k1 = (Comparable) o1[i];
          Comparable k2 = (Comparable) o2[i];
          int result = k1.compareTo(k2);
          if (result == 0) {
            continue;
          } else {
            return result;
          }
        }
        throw new RuntimeException("");
      }
    });
  }

  private static void sortIgnoreCase(List<Object[]> allRows, int... fieldOrdinal) {
    Collections.sort(allRows, new Comparator<Object[]>() {
      @Override
      public int compare(Object[] o1, Object[] o2) {
        int result = 0;
        for (int i : fieldOrdinal) {
          Comparable k1 = (Comparable) o1[i];
          Comparable k2 = (Comparable) o2[i];
          if (k1 == null) {
            return -1;
          }
          if (k2 == null) {
            return 1;
          }
          if (k1 instanceof String) {
            result = ((String) k1).compareToIgnoreCase((String) k2);
          } else {
            result = k1.compareTo(k2);
          }
          if (result == 0) {
            continue;
          } else {
            return result;
          }
        }
        return result;
      }
    });
  }

}
