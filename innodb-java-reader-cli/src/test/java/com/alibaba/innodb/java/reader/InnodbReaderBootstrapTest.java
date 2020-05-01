package com.alibaba.innodb.java.reader;

import com.google.common.base.Joiner;
import com.google.common.io.Files;

import com.alibaba.innodb.java.reader.cli.InnodbReaderBootstrap;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class InnodbReaderBootstrapTest {

  private String sourceIbdFilePath =
      "../innodb-java-reader/src/test/resources/testsuite/mysql57/multiple/level/tb10.ibd";

  private String createTableSqlPath = "src/test/resources/tb10.sql";

  /**
   * when debugging test case, you can set to true
   */
  private static final boolean ENABLE_CONSOLE_OUTPUT = false;

  private static SysoutInterceptor SYS_OUT_INTERCEPTOR;

  /**
   * By running the following command:
   * <pre>
   *   mysql -N -uroot -Dtest -e "select * from test.tb10" > tb10.dat
   * </pre>
   */
  private static String TB10_DATA_FILE = "src/test/resources/tb10.dat";

  /**
   * By running the following command:
   * <pre>
   *   mysql -N -uroot -Dtest -e "select * from test.tb10" >
   *   tb10.dat2 && cat tb10.dat2 | tr "\t" "," > tb10-comma-delimiter.dat
   * </pre>
   */
  private static String TB10_DATA_FILE_WITH_COMMA_DELIMITER =
      "src/test/resources/tb10-comma-delimiter.dat";

  /**
   * By running the following command:
   * <pre>
   *   mysql -uroot -Dtest -e "select * from test.tb10" > tb10-with-header.dat
   * </pre>
   */
  private static String TB10_DATA_FILE_WITH_HEADER =
      "src/test/resources/tb10-with-header.dat";

  private static String EXPECTED_TB10_DATE_FILE_MD5;
  private static String EXPECTED_TB10_DATE_FILE_WITH_COMMA_DELIMITER_MD5;
  private static String EXPECTED_TB10_DATE_FILE_WITH_HEADER_MD5;

  @BeforeClass
  public static void prepareWhenClassInitiate() throws IOException {
    PrintStream origOut = System.out;
    SYS_OUT_INTERCEPTOR = new SysoutInterceptor(origOut, ENABLE_CONSOLE_OUTPUT);
    System.setOut(SYS_OUT_INTERCEPTOR);
    EXPECTED_TB10_DATE_FILE_MD5 = getFileMD5(TB10_DATA_FILE);
    EXPECTED_TB10_DATE_FILE_WITH_COMMA_DELIMITER_MD5 = getFileMD5(TB10_DATA_FILE_WITH_COMMA_DELIMITER);
    EXPECTED_TB10_DATE_FILE_WITH_HEADER_MD5 = getFileMD5(TB10_DATA_FILE_WITH_HEADER);
  }

  @Before
  public void prepare() {
    SYS_OUT_INTERCEPTOR.clear();
  }

  @Test
  public void testShowAllPages() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath,
        "-create-table-sql-file-path", createTableSqlPath,
        "-c", "show-all-pages"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(577));
    assertThat(output.get(1), is("0,FILE_SPACE_HEADER,space=50,numPagesUsed=36,size=576,xdes.size=2"));
    assertThat(output.get(2), is("1,IBUF_BITMAP"));
    assertThat(output.get(3), is("2,INODE,inode.size=2"));
    assertThat(output.get(4),
        is("3,INDEX,root.page=true,index.id=66,level=1,numOfRecs=39,num.dir.slot=10,garbage.space=0"));
    assertThat(output.get(5), is("4,INDEX,index.id=66,level=0,numOfRecs=13,num.dir.slot=5,garbage.space=7501"));
    assertThat(output.get(37), is("36,ALLOCATED"));
  }

  @Test
  public void testShowPages() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "show-pages", "-args", "3,4,5"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(3));
    assertThat(output.get(0).startsWith("Index(indexHeader=IndexHeader(numOfDirSlots=10, "
            + "heapTopPosition=627, numOfHeapRecords=41, format=COMPACT"),
        is(true));
  }

  @Test
  public void testShowPagesJsonPrettyStyle() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-json-pretty-style", "-c", "show-pages", "-args", "3,4,5"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(3));
  }

  @Test
  public void testQueryAll() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "query-all"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(1000));
    check(output, 0, 1000);
  }

  @Test
  public void testQueryByPageNumberNonLeafPage() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "query-by-page-number", "-args", "3", "-delimiter", ","};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(39));
    assertThat(output.get(1), is(String.valueOf("14,null,null,null")));
  }

  @Test
  public void testQueryByPageNumberLeafPage() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "query-by-page-number", "-args", "64"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(26));
    check(output, 820, 26);
  }

  @Test
  public void testQueryByPrimaryKey() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "query-by-pk", "-args", "888"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(1));
    check(output, 888, 1);
  }

  @Test
  public void testQueryByPrimaryKeyNotExist() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "query-by-pk", "-args", "999999"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(0));
  }

  /**
   * print java.lang.IllegalArgumentException:
   * argument number should not exactly two, delimited by ; 700,800
   */
  @Test
  public void testRangeQueryByPrimaryKeyNegative() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "range-query-by-pk", "-args", "700,800"};
    InnodbReaderBootstrap.main(args);
  }

  @Test
  public void testRangeQueryByPrimaryKey() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "range-query-by-pk", "-args", ">=;700;<;800"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(100));
    check(output, 700, 100);
  }

  @Test
  public void testRangeQueryByPrimaryKey2() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "range-query-by-pk", "-args", "\">=;700;<;800\""};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(100));
    check(output, 700, 100);
  }

  @Test
  public void testRangeQueryByPrimaryKey3() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "range-query-by-pk", "-args", ">;700;nop;null"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(300));
    check(output, 700, 300);
  }

  @Test
  public void testGenLsnHeatmap() throws IOException {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "gen-lsn-heatmap", "-args", "/tmp/lsn-heatmap-output.html 800 1000"};
    InnodbReaderBootstrap.main(args);
    List<String> fileContent = Files.readLines(new File("/tmp/lsn-heatmap-output.html"), Charset.defaultCharset());
    assertThat(fileContent.get(0), is("<head>"));
    assertThat(fileContent.get(1054).trim(), is("97"));
  }

  @Test
  public void testGenFillingRateHeatmap() throws IOException {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "gen-filling-rate-heatmap", "-args", "/tmp/lsn-filling-rate-output.html 800 1000"};
    InnodbReaderBootstrap.main(args);
    List<String> fileContent = Files.readLines(new File("/tmp/lsn-filling-rate-output.html"),
        Charset.defaultCharset());
    assertThat(fileContent.get(0), is("<head>"));
    assertThat(fileContent.get(1067).trim(), is("0.466"));
  }

  @Test
  public void testQueryAllBufferIO() throws IOException {
    testIOTemplate("/tmp/query-all-buffer-io.out", "buffer",
        EXPECTED_TB10_DATE_FILE_MD5, false);
  }

  @Test
  public void testQueryAllMmapIO() throws IOException {
    testIOTemplate("/tmp/query-all-mmap-io.out", "mmap",
        EXPECTED_TB10_DATE_FILE_MD5, false);
  }

  /**
   * enable dio in travis ci environment, please refer to <code>.travis.yml</code>
   */
  @Test
  public void testQueryAllDirectIO() throws IOException {
    if (System.getenv("support_dio") != null) {
      testIOTemplate("/tmp/query-all-direct-io.out", "direct",
          EXPECTED_TB10_DATE_FILE_MD5, false);
    } else {
      System.out.println("system does not support dio, so skip test");
    }
  }

  @Test
  public void testQueryAllShowHeader() throws IOException {
    testIOTemplate("/tmp/query-all-mmap-io.out", "mmap",
        EXPECTED_TB10_DATE_FILE_WITH_HEADER_MD5, true);
  }

  @Test
  public void testQueryAllCommaDelimiter() throws IOException {
    testIOTemplate("/tmp/query-all-mmap-io.out", "mmap",
        EXPECTED_TB10_DATE_FILE_WITH_COMMA_DELIMITER_MD5, false, ",");
  }

  @Test
  public void testGetAllIndexPageFillingRate() {
    String[] args = {"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
        "-c", "get-all-index-page-filling-rate"};
    InnodbReaderBootstrap.main(args);
    List<String> output = SYS_OUT_INTERCEPTOR.getOutput();
    assertThat(output.size(), is(2));
    assertThat(output.get(1).startsWith("Index page filling rate is 0.8898"), is(true));
  }

  private void testIOTemplate(String outputFilePath, String ioMode,
                              String expectedFileMd5, boolean showHeader) throws IOException {
    testIOTemplate(outputFilePath, ioMode, expectedFileMd5, showHeader, "\t");
  }

  private void testIOTemplate(String outputFilePath, String ioMode,
                              String expectedFileMd5, boolean showHeader, String delimiter) throws IOException {
    String[] args;
    if (showHeader) {
      args = new String[]{"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
          "-showheader", "-c", "query-all", "-o", outputFilePath, "-iomode", ioMode};
    } else {
      args = new String[]{"-ibd-file-path", sourceIbdFilePath, "-create-table-sql-file-path", createTableSqlPath,
          "-c", "query-all", "-o", outputFilePath, "-iomode", ioMode};
    }
    if (!"\t".equals(delimiter)) {
      String[] newArgs = Arrays.copyOf(args, args.length + 2);
      newArgs[args.length] = "-delimiter";
      newArgs[args.length + 1] = ",";
      args = newArgs;
    }
    InnodbReaderBootstrap.main(args);
    List<String> fileContent = Files.readLines(new File(outputFilePath), Charset.defaultCharset());
    if (showHeader) {
      assertThat(fileContent.size(), is(1001));
      assertThat(fileContent.get(0), is(Joiner.on(delimiter).join(new String[]{"id", "a", "b", "c"})));
      fileContent.remove(0);
    } else {
      assertThat(fileContent.size(), is(1000));
    }
    check(fileContent, 0, 1000, delimiter);
    String md5 = getFileMD5(outputFilePath);
    System.out.println(md5);
    assertThat(md5, is(expectedFileMd5));
  }

  private void check(List<String> output, int start, int count) {
    check(output, start, count, "\t");
  }

  private void check(List<String> output, int start, int count, String delimiter) {
    for (int i = start + 1; i <= count; i++) {
      String[] array = output.get(i - start - 1).split(delimiter);
      assertThat(array[0], is(String.valueOf(i)));
      assertThat(array[1], is(String.valueOf(i * 2)));
      assertThat(array[2], is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 32))));
      assertThat(array[3], is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 512))));
    }
  }

  public static String getFileMD5(String filePath) throws IOException {
    try (InputStream is = java.nio.file.Files.newInputStream(Paths.get(filePath))) {
      return org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
    }
  }

}
