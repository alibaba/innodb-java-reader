package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.AllocatedPage;
import com.alibaba.innodb.java.reader.page.FilHeader;
import com.alibaba.innodb.java.reader.page.PageType;
import com.alibaba.innodb.java.reader.page.SdiPage;
import com.alibaba.innodb.java.reader.page.fsphdr.FspHdrXes;
import com.alibaba.innodb.java.reader.page.ibuf.IbufBitmap;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.page.index.RecordHeader;
import com.alibaba.innodb.java.reader.page.index.RecordType;
import com.alibaba.innodb.java.reader.page.inode.Inode;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import static com.alibaba.innodb.java.reader.page.index.PageFormat.COMPACT;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class SimpleTableReaderTest extends AbstractTest {

  private String createSql = "CREATE TABLE `tb01`\n"
      + "(`id` int(11) NOT NULL ,\n"
      + "`a` bigint(20) NOT NULL,\n"
      + "`b` varchar(64) NOT NULL,\n"
      + "`c` varchar(1024) default 'THIS_IS_DEFAULT_VALUE',\n"
      + "PRIMARY KEY (`id`))\n"
      + "ENGINE=InnoDB;";

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(true));
  }

  //==========================================================================
  // readAllPages test
  //==========================================================================

  @Test
  public void testSimpleTableReadAllPagesMysql56() {
    testSimpleTableReadAllPages(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableReadAllPagesMysql57() {
    testSimpleTableReadAllPages(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableReadAllPagesMysql80() {
    testSimpleTableReadAllPages(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd", true);
  }

  public void testSimpleTableReadAllPages(String path, boolean isMysql8) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check read all pages function
      List<AbstractPage> pages = reader.readAllPages();
      for (AbstractPage page : pages) {
        System.out.println(page);
      }
      // by default small table consumes 96kb which is 6 pages for mysql56 and mysql57
      // for mysql8 there is SDI page
      if (isMysql8) {
        assertThat(pages.size(), is(7));
      } else {
        assertThat(pages.size(), is(6));
      }

      // check page type
      assertThat(((FspHdrXes) pages.get(0)).getInnerPage().pageType(), is(PageType.FILE_SPACE_HEADER));
      assertThat(((IbufBitmap) pages.get(1)).getInnerPage().pageType(), is(PageType.IBUF_BITMAP));
      assertThat(((Inode) pages.get(2)).getInnerPage().pageType(), is(PageType.INODE));
      if (isMysql8) {
        assertThat(((SdiPage) pages.get(3)).getInnerPage().pageType(), is(PageType.SDI));
        assertThat(((Index) pages.get(4)).getInnerPage().pageType(), is(PageType.INDEX));
      } else {
        assertThat(((Index) pages.get(3)).getInnerPage().pageType(), is(PageType.INDEX));
        assertThat(((AllocatedPage) pages.get(4)).getInnerPage().pageType(), is(PageType.ALLOCATED));
      }
      assertThat(((AllocatedPage) pages.get(5)).getInnerPage().pageType(), is(PageType.ALLOCATED));

      // check fsp hdr
      if (isMysql8) {
        assertThat(((FspHdrXes) pages.get(0)).getFspHeader().getSize(), is(7L));
        assertThat(((FspHdrXes) pages.get(0)).getFspHeader().getNumberOfPagesUsed(), is(5L));
      } else {
        assertThat(((FspHdrXes) pages.get(0)).getFspHeader().getSize(), is(6L));
        assertThat(((FspHdrXes) pages.get(0)).getFspHeader().getNumberOfPagesUsed(), is(4L));
      }
      assertThat(((FspHdrXes) pages.get(0)).getInnerPage().getPageNumber(), is(0L));

      // check index
      Index rootPage = (Index) (isMysql8 ? pages.get(4) : pages.get(3));
      assertThat(rootPage.getIndexHeader().getPageLevel(), is(0));
      assertThat(rootPage.getIndexHeader().getNumOfRecs(), is(10));
      assertThat(rootPage.getIndexHeader().getNumOfHeapRecords(), is(12));
      assertThat(rootPage.getIndexHeader().getFormat(), is(COMPACT));
      assertThat(rootPage.getIndexHeader().getNumOfRecs(), is(10));
      assertThat(rootPage.getIndexHeader().getNumOfHeapRecords(), is(12));

      // for root page there is no prev and next page
      assertThat(rootPage.getFilHeader().getPrevPage(), nullValue());
      assertThat(rootPage.getFilHeader().getNextPage(), nullValue());
    }
  }

  //==========================================================================
  // getPageIterator test
  //==========================================================================

  @Test
  public void testSimpleTableGetPageIteratorMysql56() {
    testSimpleTableGetPageIterator(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableGetPageIteratorMysql57() {
    testSimpleTableGetPageIterator(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableGetPageIteratorMysql80() {
    testSimpleTableGetPageIterator(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd", true);
  }

  public void testSimpleTableGetPageIterator(String path, boolean isMysql8) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check read all pages function
      Iterator<AbstractPage> pageIter = reader.getPageIterator();

      while (pageIter.hasNext()) {
        AbstractPage page = pageIter.next();
        System.out.println(page);
      }
    }
  }

  //==========================================================================
  // readAllPageHeaders test
  //==========================================================================

  @Test
  public void testSimpleTableReadAllPageHeadersMysql56() {
    testSimpleTableReadAllPageHeaders(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableReadAllPageHeadersMysql57() {
    testSimpleTableReadAllPageHeaders(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableReadAllPageHeadersMysql80() {
    testSimpleTableReadAllPageHeaders(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd", true);
  }

  public void testSimpleTableReadAllPageHeaders(String path, boolean isMysql8) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      List<FilHeader> pageHeaders = reader.readAllPageHeaders();
      for (FilHeader pageHeader : pageHeaders) {
        System.out.println(pageHeader);
      }
      // by default small table consumes 96kb which is 6 pages for mysql56 and mysql57
      // for mysql8 there is SDI page
      if (isMysql8) {
        assertThat(pageHeaders.size(), is(7));
      } else {
        assertThat(pageHeaders.size(), is(6));
      }

      // check page type
      assertThat(pageHeaders.get(0).getPageType(), is(PageType.FILE_SPACE_HEADER));
      assertThat(pageHeaders.get(1).getPageType(), is(PageType.IBUF_BITMAP));
      assertThat(pageHeaders.get(2).getPageType(), is(PageType.INODE));
      if (isMysql8) {
        assertThat(pageHeaders.get(3).getPageType(), is(PageType.SDI));
        assertThat(pageHeaders.get(4).getPageType(), is(PageType.INDEX));
      } else {
        assertThat(pageHeaders.get(3).getPageType(), is(PageType.INDEX));
        assertThat(pageHeaders.get(4).getPageType(), is(PageType.ALLOCATED));
      }
      assertThat(pageHeaders.get(5).getPageType(), is(PageType.ALLOCATED));
    }
  }

  //==========================================================================
  // readPage test
  //==========================================================================

  @Test
  public void testSimpleTableReadPageMysql56() {
    testSimpleTableReadPage(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableReadPageMysql57() {
    testSimpleTableReadPage(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableReadPageMysql80() {
    testSimpleTableReadPage(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd", true);
  }

  public void testSimpleTableReadPage(String path, boolean isMysql8) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check readPage function
      AbstractPage index = reader.readPage(isMysql8 ? 4 : 3);
      assertThat(((Index) index).getIndexHeader().getPageLevel(), is(0));
      assertThat(((Index) index).getIndexHeader().getNumOfRecs(), is(10));
      assertThat(((Index) index).getIndexHeader().getNumOfHeapRecords(), is(12));
      assertThat(((Index) index).getIndexHeader().getFormat(), is(COMPACT));
    }
  }

  //==========================================================================
  // queryByPageNumber verifying header test
  //==========================================================================

  @Test
  public void testSimpleTableQueryByPageNumberVerifyHeaderMysql56() {
    testSimpleTableQueryByPageNumberVerifyHeader(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableQueryByPageNumberVerifyHeaderMysql57() {
    testSimpleTableQueryByPageNumberVerifyHeader(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableQueryByPageNumberVerifyHeaderMysql80() {
    testSimpleTableQueryByPageNumberVerifyHeader(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd", true);
  }

  public void testSimpleTableQueryByPageNumberVerifyHeader(String path, boolean isMysql8) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(isMysql8 ? 4L : 3);
      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        RecordHeader recordHeader = record.getHeader();
        assertThat(record.getPageNumber(), is(isMysql8 ? 4L : 3L));
        assertThat(record.getSchema() == null, is(false));
        assertThat(recordHeader.getRecordType(), is(RecordType.CONVENTIONAL));
        assertThat(recordHeader.getInfoFlag(), nullValue());
      }
    }
  }

  //==========================================================================
  // queryByPageNumber test and negative test
  //==========================================================================

  @Test
  public void testSimpleTableQueryByPageNumberMysql56() {
    testSimpleTableQueryByPageNumber(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableQueryByPageNumberMysql57() {
    testSimpleTableQueryByPageNumber(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd", false);
  }

  @Test
  public void testSimpleTableQueryByPageNumberMysql80() {
    testSimpleTableQueryByPageNumber(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd", true);
  }

  // will print Skip SDI page
  @Test
  public void testSimpleTableQueryByPageNumberMysql80FromPage3() {
    testSimpleTableQueryByPageNumber(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd", false);
  }

  public void testSimpleTableQueryByPageNumber(String path, boolean isMysql8) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      // check queryByPageNumber
      List<GenericRecord> recordList = reader.queryByPageNumber(isMysql8 ? 4 : 3);
      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assertThat(values[0], is(i));
        assertThat(values[1], is(i * 2L));
        assertThat(values[2], is(StringUtils.repeat('A', 16)));
        assertThat(values[3], is(StringUtils.repeat('C', 8) + (char) (97 + i)));

        assertThat(record.getPrimaryKey(), is(i));
        assertThat(record.get(0), is(i));
        assertThat(record.get("id"), is(i));

        assertThat(record.get(1), is(i * 2L));
        assertThat(record.get("a"), is(i * 2L));

        assertThat(record.get(2), is(StringUtils.repeat('A', 16)));
        assertThat(record.get("b"), is(StringUtils.repeat('A', 16)));

        assertThat(record.get(3), is(StringUtils.repeat('C', 8) + (char) (97 + i)));
        assertThat(record.get("c"), is(StringUtils.repeat('C', 8) + (char) (97 + i)));
      }
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testSimpleTableQueryByPageNumberNegativePage() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getSchema())) {
      reader.open();
      reader.queryByPageNumber(1);
    }
  }

  //==========================================================================
  // queryByPrimaryKey test
  //==========================================================================

  @Test
  public void testSimpleTableQueryByPrimaryKeyMysql56() {
    testSimpleTableQueryByPrimaryKey(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd");
  }

  @Test
  public void testSimpleTableQueryByPrimaryKeyMysql57() {
    testSimpleTableQueryByPrimaryKey(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd");
  }

  @Test
  public void testSimpleTableQueryByPrimaryKeyMysql80() {
    testSimpleTableQueryByPrimaryKey(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd");
  }

  public void testSimpleTableQueryByPrimaryKey(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      for (int i = 1; i <= 10; i++) {
        GenericRecord record = reader.queryByPrimaryKey(i);
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assertThat(values[0], is(i));
        assertThat(values[1], is(i * 2L));
        assertThat(values[2], is(StringUtils.repeat('A', 16)));
        assertThat(values[3], is(StringUtils.repeat('C', 8) + (char) (97 + i)));
      }

      assertThat(reader.queryByPrimaryKey(100), nullValue());
      assertThat(reader.queryByPrimaryKey(11), nullValue());
      assertThat(reader.queryByPrimaryKey(0), nullValue());
      assertThat(reader.queryByPrimaryKey(-1), nullValue());
    }
  }

  //==========================================================================
  // queryAll test
  //==========================================================================

  @Test
  public void testSimpleTableQueryAllMysql56() {
    testSimpleTableQueryAll(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd");
  }

  @Test
  public void testSimpleTableQueryAllMysql57() {
    testSimpleTableQueryAll(IBD_FILE_BASE_PATH_MYSQL57 + "simple/tb01.ibd");
  }

  @Test
  public void testSimpleTableQueryAllMysql80() {
    testSimpleTableQueryAll(IBD_FILE_BASE_PATH_MYSQL80 + "simple/tb01.ibd");
  }

  public void testSimpleTableQueryAll(String path) {
    try (TableReader reader = new TableReader(path, createSql)) {
      reader.open();

      List<GenericRecord> recordList = reader.queryAll();
      int index = 0;
      for (int i = 1; i <= 10; i++) {
        GenericRecord record = recordList.get(index++);
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assertThat(values[0], is(i));
        assertThat(values[1], is(i * 2L));
        assertThat(values[2], is(StringUtils.repeat('A', 16)));
        assertThat(values[3], is(StringUtils.repeat('C', 8) + (char) (97 + i)));
      }
    }
  }

  //==========================================================================
  // queryAll with predicate test
  //==========================================================================

  @Test
  public void testSimpleTableQueryAllWithPredicate() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH_MYSQL56 + "simple/tb01.ibd", getSchema())) {
      reader.open();

      Predicate<GenericRecord> predicate = r -> (long) (r.get("a")) == 12L;

      List<GenericRecord> recordList = reader.queryAll(predicate);
      assertThat(recordList.size(), is(1));
      assertThat(recordList.get(0).getPrimaryKey(), is(6));
      assertThat(recordList.get(0).get("a"), is(12L));
    }
  }

  //==========================================================================
  // table cannot be opened more than once
  //==========================================================================

  @Test(expected = ReaderException.class)
  public void testSimpleTableOpenTwice() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getSchema())) {
      reader.open();
      reader.open();
    }
  }

  //==========================================================================
  // table cannot be opened more than once
  //==========================================================================

  @Test
  public void testSimpleTableGetNumOfPages() {
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getSchema())) {
      reader.open();
      assertThat(reader.getNumOfPages(), is(6L));
    }
  }

  //==========================================================================
  // getIndexPageFillingRate test
  //==========================================================================

  @Test
  public void testGetIndexPageFillingRate() {
    // small table
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "simple/tb01.ibd", getSchema())) {
      reader.open();
      double fillingRate = reader.getIndexPageFillingRate(3);
      System.out.println(fillingRate);
      assertThat(String.valueOf(fillingRate).startsWith("0.04357"), is(true));
    }

    // big table
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
      reader.open();
      assertThat(String.valueOf(reader.getIndexPageFillingRate(37)).startsWith("0.7807"), is(true));
    }
  }

  //==========================================================================
  // getAllIndexPageFillingRate test
  //==========================================================================

  @Test
  public void testGetAllIndexPageFillingRate() {
    // big table
    try (TableReader reader = new TableReader(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", getSchema())) {
      reader.open();
      assertThat(String.valueOf(reader.getAllIndexPageFillingRate()).startsWith("0.9230"), is(true));
    }
  }
}
