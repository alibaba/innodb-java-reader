package com.alibaba.innodb.java.reader;

import com.google.common.base.MoreObjects;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.util.Utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author xu.zx
 */
public class AbstractTest {

  /**
   * Holds a mapping of test case to Innodb file which will used when testing.
   */
  private static final Properties TEST_CASE_PROPERTIES = loadProperties();

  public static final String IBD_FILE_BASE_PATH_MYSQL56 = TEST_CASE_PROPERTIES.getProperty("base.path.mysql56");
  public static final String IBD_FILE_BASE_PATH_MYSQL57 = TEST_CASE_PROPERTIES.getProperty("base.path.mysql57");
  public static final String IBD_FILE_BASE_PATH_MYSQL80 = TEST_CASE_PROPERTIES.getProperty("base.path.mysql80");
  public static final String IBD_FILE_BASE_PATH = IBD_FILE_BASE_PATH_MYSQL56;

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

    public AssertThat withTableDef(TableDef tableDef) {
      this.tableDef = tableDef;
      return this;
    }

    public AssertThat withSql(String createTableSql) {
      this.createTableSql = createTableSql;
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

  protected String expectedLocalTime(String dateTime) {
    ZoneOffset zoneOffset = ZonedDateTime.now().getOffset();
    LocalDateTime ldt = Utils.parseDateTimeText(dateTime);
    Instant instant = Instant.ofEpochSecond(ldt.toEpochSecond(ZoneOffset.of("+00:00")));
    OffsetDateTime odt = instant.atOffset(zoneOffset);
    return odt.format(Utils.TIME_FORMAT_TIMESTAMP[0]);
  }

}
