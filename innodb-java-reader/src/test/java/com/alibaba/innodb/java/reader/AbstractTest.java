package com.alibaba.innodb.java.reader;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.provider.impl.SqlFileTableDefProvider;
import com.alibaba.innodb.java.reader.util.ConcurrentCache;
import com.alibaba.innodb.java.reader.util.Utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
@Slf4j
public class AbstractTest {

  /**
   * Holds a mapping of test case to Innodb file which will used when testing.
   */
  private static final Properties TEST_CASE_PROPERTIES = loadProperties();

  public static final String IBD_FILE_BASE_PATH_MYSQL56 = TEST_CASE_PROPERTIES.getProperty("base.path.mysql56");
  public static final String IBD_FILE_BASE_PATH_MYSQL57 = TEST_CASE_PROPERTIES.getProperty("base.path.mysql57");
  public static final String IBD_FILE_BASE_PATH_MYSQL80 = TEST_CASE_PROPERTIES.getProperty("base.path.mysql80");
  public static final String IBD_FILE_BASE_PATH = IBD_FILE_BASE_PATH_MYSQL56;

  protected static final ConcurrentCache<Object, List<Object[]>> TABLES = new ConcurrentCache<>();

  public static final boolean[][] BOOLEAN_OPTIONS = new boolean[][]{
      {true, false}, {true, true}, {false, true}, {false, false},
  };

  protected static <T extends AbstractTest> AssertThat assertTestOf(T testInstance) {
    return new AssertThat().withTestCase(testInstance);
  }

  protected ThreadLocal<Boolean> isMysql8Flag = ThreadLocal.withInitial(() -> false);

  public static class AssertThat {

    private TableDef tableDef;

    private String createTableSql;

    private Class<?> testClass;

    private String ibdFileBasePath;

    private AbstractTest testInstance;

    private boolean enableTableDefProvider;

    public AssertThat withTableDef(TableDef tableDef) {
      this.tableDef = tableDef;
      return this;
    }

    public AssertThat withSql(String createTableSql) {
      this.createTableSql = createTableSql;
      return this;
    }

    public AssertThat withTableDefProvider() {
      this.enableTableDefProvider = true;
      return this;
    }

    public <T extends AbstractTest> AssertThat withTestCase(T testInstance) {
      this.testInstance = testInstance;
      this.testClass = testInstance.getClass();
      return this;
    }

    public AssertThat withMysql56() {
      this.ibdFileBasePath = IBD_FILE_BASE_PATH_MYSQL56;
      if (testInstance != null) {
        testInstance.isMysql8Flag.set(false);
      }
      return this;
    }

    public AssertThat withMysql57() {
      this.ibdFileBasePath = IBD_FILE_BASE_PATH_MYSQL57;
      if (testInstance != null) {
        testInstance.isMysql8Flag.set(false);
      }
      return this;
    }

    public AssertThat withMysql80() {
      this.ibdFileBasePath = IBD_FILE_BASE_PATH_MYSQL80;
      if (testInstance != null) {
        testInstance.isMysql8Flag.set(true);
      }
      return this;
    }

    public <T, RES> AssertThat doWithTableReader(Function<RES, T> fn,
                                                 Function<TableReader, RES> queryFn) {
      String ibdDataFilePath = TEST_CASE_PROPERTIES.getProperty(testClass.getSimpleName());
      if (StringUtils.isEmpty(ibdDataFilePath)) {
        throw new RuntimeException("ibd data file path is empty for test case class " + testClass.getSimpleName());
      }
      TableReader reader;
      if (tableDef != null) {
        reader = new TableReaderImpl(ibdFileBasePath + ibdDataFilePath, tableDef);
      } else if (createTableSql != null) {
        reader = new TableReaderImpl(ibdFileBasePath + ibdDataFilePath, createTableSql);
      } else if (enableTableDefProvider) {
        TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
            .withProvider(new SqlFileTableDefProvider(IBD_FILE_BASE_PATH
                + ibdDataFilePath.replace(".ibd", ".sql")))
            .withDataFilePath(ibdFileBasePath + ibdDataFilePath)
            .build();
        reader = tableReaderFactory.createTableReader(getTableName(ibdDataFilePath));
      } else {
        throw new IllegalStateException("No schema or createTableSql found");
      }

      try {
        reader.open();
        RES recordList = queryFn.apply(reader);
        T t = fn.apply(recordList);
        return this;
      } finally {
        reader.close();
      }
    }

    public AssertThat checkRootPageIs(Consumer<List<GenericRecord>> fn) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, tableReader -> tableReader.queryByPageNumber(3));
    }

    public AssertThat checkAllRecordsIs(Consumer<List<GenericRecord>> fn) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, TableReader::queryAll);
    }

    public AssertThat checkAllRecordsIs(Consumer<List<GenericRecord>> fn,
                                        List<String> projection) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.queryAll(projection));
    }

    public AssertThat checkRangeQueryRecordsIs(Consumer<List<GenericRecord>> fn,
                                               List<Object> start, ComparisonOperator startOperator,
                                               List<Object> end, ComparisonOperator endOperator) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.rangeQueryByPrimaryKey(start, startOperator, end, endOperator));
    }

    public AssertThat checkQueryAllIterator(Consumer<Iterator<GenericRecord>> fn) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, TableReader::getQueryAllIterator);
    }

    public AssertThat checkQueryAllIteratorDesc(Consumer<Iterator<GenericRecord>> fn) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.getQueryAllIterator(false));
    }

    public AssertThat checkQueryAllIteratorProjection(Consumer<Iterator<GenericRecord>> fn,
                                                      List<String> projection) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.getQueryAllIterator(projection));
    }

    public AssertThat checkQueryAllIteratorProjectionDesc(Consumer<Iterator<GenericRecord>> fn,
                                                          List<String> projection) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.getQueryAllIterator(projection, false));
    }

    public AssertThat checkRangeQueryIterator(Consumer<Iterator<GenericRecord>> fn,
                                              List<Object> start, ComparisonOperator startOperator,
                                              List<Object> end, ComparisonOperator endOperator) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.getRangeQueryIterator(start, startOperator, end, endOperator));
    }

    public AssertThat checkRangeQueryIteratorProjection(Consumer<Iterator<GenericRecord>> fn,
                                                        List<Object> start, ComparisonOperator startOperator,
                                                        List<Object> end, ComparisonOperator endOperator,
                                                        List<String> projection) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.getRangeQueryIterator(start, startOperator, end, endOperator, projection));
    }

    public AssertThat checkRangeQueryIteratorProjectionDesc(Consumer<Iterator<GenericRecord>> fn,
                                                            List<Object> start, ComparisonOperator startOperator,
                                                            List<Object> end, ComparisonOperator endOperator,
                                                            List<String> projection) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.getRangeQueryIterator(start, startOperator, end, endOperator, projection, false));
    }

    public AssertThat checkQueryByPk(Consumer<GenericRecord> fn, List<Object> key) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> t.queryByPrimaryKey(key));
    }

    public AssertThat checkQueryIteratorBySk(Consumer<Iterator<GenericRecord>> fn,
                                             String skName,
                                             List<Object> start, ComparisonOperator startOperator,
                                             List<Object> end, ComparisonOperator endOperator) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> {
        log.info("---- {}, {},{} and {},{}", skName, startOperator, start, endOperator, end);
        return t.getRecordIteratorBySk(skName, start, startOperator, end, endOperator);
      });
    }

    public AssertThat checkQueryIteratorBySkProjection(Consumer<Iterator<GenericRecord>> fn,
                                                       String skName, List<String> projection,
                                                       List<Object> start, ComparisonOperator startOperator,
                                                       List<Object> end, ComparisonOperator endOperator) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> {
        log.info("---- {}, {},{} and {},{}", skName, startOperator, start, endOperator, end);
        return t.getRecordIteratorBySk(skName, start, startOperator, end, endOperator, projection);
      });
    }

    public AssertThat checkQueryIteratorBySkDesc(Consumer<Iterator<GenericRecord>> fn,
                                                 String skName,
                                                 List<Object> start, ComparisonOperator startOperator,
                                                 List<Object> end, ComparisonOperator endOperator) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> {
        log.info("---- {}, {},{} and {},{}", skName, startOperator, start, endOperator, end);
        return t.getRecordIteratorBySk(skName, start, startOperator, end, endOperator, false);
      });
    }

    public AssertThat checkQueryIteratorBySkProjectionDesc(Consumer<Iterator<GenericRecord>> fn,
                                                           String skName, List<String> projection,
                                                           List<Object> start, ComparisonOperator startOperator,
                                                           List<Object> end, ComparisonOperator endOperator) {
      return doWithTableReader(c -> {
        fn.accept(c);
        return this;
      }, t -> {
        log.info("---- {}, {},{} and {},{}", skName, startOperator, start, endOperator, end);
        return t.getRecordIteratorBySk(skName, start, startOperator, end, endOperator, projection, false);
      });
    }
  }

  protected byte[] getContent(byte prefix, byte b, int repeatB) {
    return getContent(prefix, b, repeatB, repeatB + 1);
  }

  protected byte[] getContent(byte prefix, byte b, int repeatB, int len) {
    ByteBuffer buffer = ByteBuffer.allocate(len);
    buffer.put(prefix);
    for (int i = 0; i < repeatB; i++) {
      buffer.put(b);
    }
    if (buffer.remaining() > 0) {
      for (int i = 0; i < buffer.remaining(); i++) {
        buffer.put((byte) 0x00);
      }
    }
    return buffer.array();
  }

  private static Properties loadProperties() {
    Properties testCaseProperties = new Properties();
    ClassLoader classLoader = MoreObjects.firstNonNull(
        Thread.currentThread().getContextClassLoader(),
        AbstractTest.class.getClassLoader());
    try (InputStream stream = classLoader.getResourceAsStream("testcase.properties")) {
      if (stream != null) {
        testCaseProperties.load(stream);
      }
    } catch (IOException e) {
      throw new RuntimeException("while reading from testcase.properties file", e);
    }
    return testCaseProperties;
  }

  protected static String expectedLocalTime(String dateTime) {
    ZoneRules rules = ZoneId.systemDefault().getRules();
    ZoneOffset standardOffset = rules.getStandardOffset(Instant.now());
    LocalDateTime ldt = Utils.parseDateTimeText(dateTime);
    OffsetDateTime odt = ldt.toInstant(ZoneOffset.of("+00:00")).atOffset(standardOffset);
    return odt.format(Utils.TIME_FORMAT_TIMESTAMP[0]);
  }

  public static class HrSchema {

    public static final Employee[] EMPS = {
        new Employee(1, 100L, "Eric", 20, "M", "1983-10-23",
            "New York", 52000, 30, "2020-01-01 18:35:40", 6, "",
            null, "eric@test.com"),
        new Employee(2, 101L, "Neo", 10, "M", "1986-10-02",
            "Berlin", 68000, 33, "2018-04-09 09:00:00", 8, "",
            "main street", "neo@test.com"),
        new Employee(3, 102L, "Sarah", 20, "F", "1990-07-25",
            "LA", 20000, 27, "2019-11-16 10:26:40", 4, "Hello world",
            null, "sarah@test.com"),
        new Employee(4, 105L, "Json", 30, "M", "1959-02-14",
            "Beijing", 100000, 60, "2015-03-09 22:16:30", 12, "Start",
            null, "json@test.com"),
        new Employee(5, 106L, "SMITH", 10, "M", "1981-01-05",
            "Tokyo", 39000, 25, "2018-09-02 12:12:56", 6, "",
            null, "smith@test.com"),
        new Employee(6, 107L, "lucy", 40, "F", "1989-06-07",
            "New York", 40000, 30, "2018-06-01 14:45:00", 5, StringUtils.repeat("p", 1000),
            null, "lucy@test.com"),
        new Employee(7, 108L, "JAMES", 20, "M", "1992-05-06",
            "LA", 29000, 20, "2017-08-18 23:11:06", 3, "",
            null, "james@test.com"),
        new Employee(8, 109L, "John", 40, "M", "1989-06-07",
            "New York", 32000, 42, "2018-06-01 14:45:00", 6, "",
            null, "john@test.com"),
        new Employee(9, 110L, "MILLER", 30, "F", "1982-07-04",
            "New York", 68000, 40, "2020-01-02 12:19:00", 7, "",
            null, "miller@test.com"),
        new Employee(10, 111L, "Jane", 20, "F", "1995-08-29",
            "LA", 19000, 22, "2019-09-30 02:14:56", 5, "",
            null, "jane@test.com"),
        new Employee(11, 112L, "Sarah", 40, "F", "1988-11-23",
            "New York", 21000, 26, "2017-04-27 16:27:11", 7, StringUtils.repeat("apple", 40),
            null, "sarah02@test.com"),
        new Employee(12, 113L, "Paul", 30, "M", "1984-11-06",
            "Berlin", 20000, 43, "2019-07-28 12:12:12", 9, "",
            null, "paul@test.com"),
        new Employee(13, 114L, "Lara", 20, "F", "1987-01-21",
            "Beijing", 35000, 29, "2018-06-08 12:12:12", 6, "",
            "老北京胡同Z区", "lara@test.com"),
        new Employee(14, 115L, "ADAMS", 20, "M", "1993-04-15",
            "LA", 38000, 35, "2019-06-08 12:12:12", 5, "",
            "LA 001", "adams@test.com"),
        new Employee(15, 116L, "SMITH", 30, "M", "1986-07-25",
            "Beijing", 55000, 36, "2017-08-17 22:01:37", 8, "",
            null, "smith02@test.com"),
        new Employee(16, 120L, "Scott", 20, "M", "1990-03-04",
            "Berlin", 33000, 31, "2018-08-17 22:01:37", 7, "",
            null, "scott@test.com"),
        new Employee(17, 121L, "MARTIN", 10, "F", "1975-02-28",
            "Tokyo", 63000, 45, "2017-06-09 12:01:37", 9, "",
            null, "martin@test.com"),
        new Employee(18, 122L, "kidd", 20, "m", "1988-05-17",
            "new York", 37000, 29, "2019-12-31 00:15:30", 5, StringUtils.repeat("phone", 50),
            "Queen zone", "kidd@test.com"),
        new Employee(19, 123L, "Yue", 30, "F", "1979-11-09",
            "New York", 57000, 37, "2017-03-04 16:16:32", 7, "",
            null, "yue@test.com"),
        new Employee(new Integer(20), new Long(124L), "Oscar", 20, "M", "1988-10-08",
            "LA", 36000, 27, "2018-03-04 16:16:32", 6, "",
            null, "oscar@test.com")
    };

    public static final Department[] DEPTS = {
        new Department(10, "ACCOUNTING"),
        new Department(20, "RESEARCH"),
        new Department(30, "SALES"),
        new Department(40, "OPERATIONS")
    };
  }

  @Data
  public static class Employee {
    public final int id;
    public final long empno;
    public final String name;
    public final int deptno;
    public final String gender;
    public final String birthdate;
    public final String city;
    public final int salary;
    public final int age;
    public final String joindate;
    public final int level;
    @ToString.Exclude
    public final String profile;
    public final String address;
    public final String email;
  }

  @Data
  public static class Department {
    public final int deptno;
    public final String name;
  }

  protected static <T> List<Object[]> getAllRows(T[] array) {
    return TABLES.get(array, () -> {
      Class<?> clazz = array.getClass().getComponentType();
      Field[] fields = getInstanceFields(clazz);
      List<Object[]> result = new ArrayList<>(array.length);
      for (int i = 0; i < array.length; i++) {
        Object[] temp = new Object[fields.length];
        for (int j = 0; j < fields.length; j++) {
          try {
            // format timestamp to local time
            if (j == 9) {
              temp[j] = expectedLocalTime((String) fields[j].get(array[i]));
            } else {
              temp[j] = fields[j].get(array[i]);
            }
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
        result.add(temp);
      }
      return result;
    });
  }

  protected static Field[] getInstanceFields(Class<?> clazz) {
    List<Field> fields = new ArrayList<>();
    for (Class<?> itr = clazz; hasSuperClass(itr); ) {
      for (Field field : itr.getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers())) {
          fields.add(field);
        }
      }
      itr = itr.getSuperclass();
    }

    return fields.toArray(new Field[fields.size()]);
  }

  protected static int getFieldOrdinal(Class<?> clazz, String fieldName) {
    int ordinal = 0;
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      if (field.getName().equals(fieldName)) {
        return ordinal;
      }
      ++ordinal;
    }

    return -1;
  }

  protected static boolean hasSuperClass(Class<?> clazz) {
    return (clazz != null) && !clazz.equals(Object.class);
  }

  protected Consumer<Iterator<GenericRecord>> expectedRangeQueryPartially(int start, int a1,
                                                                          int end, int a2) {
    return iterator -> {
      checkData(iterator, start, end, a1, a2);
    };
  }

  protected Consumer<List<GenericRecord>> expectedRangeQueryPartially2(int start, int a1,
                                                                       int end, int a2) {
    return recordList -> {
      checkData(recordList.iterator(), start, end, a1, a2);
    };
  }

  protected int checkData(Iterator<GenericRecord> iterator, int start, int end) {
    // end is exclusive, so add 1
    return checkData(iterator, start, end, 1, 4 + 1);
  }

  protected int checkData(List<GenericRecord> recordList, int start, int end) {
    // end is exclusive, so add 1
    return checkData(recordList.iterator(), start, end, 1, 4 + 1);
  }

  /**
   * Data looks like below, 4 rows in a group,
   * <pre>
   * [m, 1, m, m, 1000m, 1m, 1569985199, elvWbKt]
   * [m, 1, m, m, 1000m, 2m, 1569985199, CaCwkBP]
   * [m, 2, m, m, 1000m, 1m, 1569985199, rUsgZPyBCKT]
   * [m, 2, m, m, 1000m, 2m, 1569985199, VEsK]
   * [nn, 1, nn, nn, 1001nn, 1nn, 1569985199, VGqsgjPjq]
   * [nn, 1, nn, nn, 1001nn, 2nn, 1569985199, QoeEXxq]
   * [nn, 2, nn, nn, 1001nn, 1nn, 1569985199, mhrp]
   * [nn, 2, nn, nn, 1001nn, 2nn, 1569985199, bMQZJsN]
   * ...
   * </pre>
   * Primary key is
   * <pre>
   *   [1000mm, 1, 1m]
   *   [1000mm, 1, 2m]
   *   [1000mm, 2, 1m]
   *   [1000mm, 2, 2m]
   *   ...
   * </pre>
   */
  protected int checkData(Iterator<GenericRecord> iterator, int start, int end, int a1, int a2) {
    int count = 0;
    System.out.println("check [" + start + ", " + end + ") " + a1 + " " + a2);
    for (int i = start; i < end; i++) {
      for (int j = 1; j <= 2; j++) {
        flag:
        for (int k = 1; k <= 2; k++) {
          int samePrefixElemIdx = (j - 1) * 2 + k;
          // check row count in a group, a1 and a2 is the lower (inclusive) and
          // upper (inclusive) bounds.
          if (i == start && samePrefixElemIdx < a1) {
            continue flag;
          }
          // check 2nd column in primary key
          if (i == end - 1 && samePrefixElemIdx > a2) {
            continue flag;
          }
          if (iterator.hasNext()) {
            GenericRecord r = iterator.next();
            // System.out.println(Arrays.toString(r.getValues()));
            assertThat(r.getPrimaryKey().isEmpty(), is(false));
            if (r.getTableDef().getPrimaryKeyColumnNum() > 1) {
              assertThat(r.getPrimaryKey(), is(ImmutableList.of(
                  i + StringUtils.repeat((char) (97 + i % 26), i % 10 + 1),
                  j,
                  k + StringUtils.repeat((char) (97 + i % 26), i % 5 + 1)
              )));
            } else {
              assertThat(r.getPrimaryKey(), is(ImmutableList.of(r.get("c8"))));
            }
            assertThat(r.get("c1"), is(StringUtils.repeat((char) (97 + i % 26), i % 20 + 1)));
            assertThat(r.get("c2"), is(j));
            assertThat(r.get("c3"), is(StringUtils.repeat((char) (97 + i % 26), i % 10 + 1)));
            assertThat(r.get("c4"), is(StringUtils.repeat((char) (97 + i % 26), i % 4 + 1)));
            assertThat(r.get("c5"), is(i + StringUtils.repeat((char) (97 + i % 26), i % 10 + 1)));
            assertThat(r.get("c6"), is(k + StringUtils.repeat((char) (97 + i % 26), i % 5 + 1)));
            assertThat(r.get("c7"), is(expectedLocalTime("2019-10-02 02:59:59")));
            assertThat(r.get("c8").getClass().isAssignableFrom(String.class),
                is(true));
            assertThat(StringUtils.isAlpha(r.get("c8").toString()),
                is(true));
            assertThat(r.get("c8").toString().length(),
                greaterThanOrEqualTo(4));
            count++;
          }
        }
      }
    }
    assertThat(iterator.hasNext(), is(false));
    System.out.println(count);
    // assert row count,
    // start, end, same group simply a2 - a1
    // start, mid, end,
    // start, mid1, mid2, ... end, when a2 = 1, there is a special case
    if (end - start == 1) {
      assertThat(count, is(a2 - a1));
    } else if (end - start == 2) {
      assertThat(count, is((5 - a1) + a2 - 1));
    } else {
      if (a2 == 1) {
        assertThat(count, is((end - start - 2) * 4 + (5 - a1)));
      } else {
        assertThat(count, is((end - start - 2) * 4 + (5 - a1) + a2 - 1));
      }
    }
    return count;
  }

  private static String getTableName(String ibdDataFilePath) {
    int indexOfSlash = ibdDataFilePath.lastIndexOf("/");
    String result = ibdDataFilePath;
    if (indexOfSlash >= 0) {
      result = result.substring(indexOfSlash + 1);
    }
    int indexOfDot = result.indexOf(".");
    if (indexOfDot >= 0) {
      result = result.substring(0, indexOfDot);
    }
    return result;
  }

}
