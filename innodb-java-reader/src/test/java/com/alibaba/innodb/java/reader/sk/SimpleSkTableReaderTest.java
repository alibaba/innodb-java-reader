package com.alibaba.innodb.java.reader.sk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

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
      + "  `profile` text NOT NULL,\n"
      + "  `address` varchar(500) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"
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

  @Test(expected = ClassCastException.class)
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
