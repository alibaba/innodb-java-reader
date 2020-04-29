package com.alibaba.innodb.java.reader.multiple.level;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderImpl;
import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.PageType;
import com.alibaba.innodb.java.reader.page.fsphdr.FspHdrXes;
import com.alibaba.innodb.java.reader.page.ibuf.IbufBitmap;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.page.inode.Inode;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class BPlusTreeMultipleLevelTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(false));
  }

  @Test
  public void testBPlusTreeLevelTableReadAllPagesMysql56() {
    testBPlusTreeLevelTableReadAllPages(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb10.ibd", false);
  }

  @Test
  public void testBPlusTreeLevelTableReadAllPagesMysql57() {
    testBPlusTreeLevelTableReadAllPages(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb10.ibd", false);
  }

  @Test
  public void testBPlusTreeLevelTableReadAllPagesMysql80() {
    testBPlusTreeLevelTableReadAllPages(IBD_FILE_BASE_PATH_MYSQL80 + "multiple/level/tb10.ibd", true);
  }

  public void testBPlusTreeLevelTableReadAllPages(String path, boolean isMysql8) {
    testNLevelTableReadAllPages(path, 1, isMysql8);
  }

  @Test
  public void testPlusTree2LevelTableReadAllPages() {
    testNLevelTableReadAllPages(IBD_FILE_BASE_PATH + "multiple/level/tb11.ibd", 2, false);
  }

  public void testNLevelTableReadAllPages(String path, int maxLevel, boolean isMysql8) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      // check read all pages function
      List<AbstractPage> pages = reader.readAllPages();
      for (AbstractPage page : pages) {
        if (page.getInnerPage().pageType().equals(PageType.ALLOCATED)) {
          continue;
        }
        //System.out.println(page);
      }
      //assertThat(pages.size(), is(576));

      assertThat(((FspHdrXes) pages.get(0)).getInnerPage().pageType(), is(PageType.FILE_SPACE_HEADER));
      assertThat(((IbufBitmap) pages.get(1)).getInnerPage().pageType(), is(PageType.IBUF_BITMAP));
      assertThat(((Inode) pages.get(2)).getInnerPage().pageType(), is(PageType.INODE));
      assertThat(((Index) pages.get(isMysql8 ? 4 : 3)).getInnerPage().pageType(), is(PageType.INDEX));
      assertThat(((Index) pages.get(isMysql8 ? 4 : 3)).getIndexHeader().getPageLevel(), is(maxLevel));
      // 32 bitmap fragArrayEntries
      for (int i = isMysql8 ? 5 : 4; i <= 35; i++) {
        assertThat(((Index) pages.get(i)).getIndexHeader().getPageLevel(), is(0));
      }
    }
  }

  @Test
  public void testMultipleLevelTableQueryAll1000Mysql56() {
    testMultipleLevelTableQueryAll1000(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb10.ibd");
  }

  @Test
  public void testMultipleLevelTableQueryAll1000Mysql57() {
    testMultipleLevelTableQueryAll1000(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb10.ibd");
  }

  @Test
  public void testMultipleLevelTableQueryAll1000Mysql80() {
    testMultipleLevelTableQueryAll1000(IBD_FILE_BASE_PATH_MYSQL80 + "multiple/level/tb10.ibd");
  }

  public void testMultipleLevelTableQueryAll1000(String path) {
    testMultipleLevelTableQueryAll1000(path, 1, 1000);
  }

  public void testMultipleLevelTableQueryAll1000(String path, int start, int end) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(end - start + 1));
      int index = 0;
      for (int i = 1; i <= recordList.size(); i++) {
        GenericRecord record = recordList.get(index++);
        //System.out.println(record);
        assertThat(record.get("a"), is(i * 2L));
        assertThat(record.get("b"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 32))));
        assertThat(record.get("c"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 512))));
      }
    }
  }

  @Test
  public void testMultipleLevelTableQueryAll40000Mysql56() {
    testMultipleLevelTableQueryAll40000(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
  }

  @Test
  public void testMultipleLevelTableQueryAll40000Mysql57() {
    testMultipleLevelTableQueryAll40000(IBD_FILE_BASE_PATH_MYSQL57 + "multiple/level/tb11.ibd");
  }

  public void testMultipleLevelTableQueryAll40000(String path) {
    try (TableReader reader = new TableReaderImpl(path, getTableDef())) {
      reader.open();

      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(40000));
      int index = 0;
      for (int i = 1; i <= 20000; i++) {
        GenericRecord record = recordList.get(index++);
        //System.out.println(record);
        assertThat(record.get("a"), is(i * 2L));
        assertThat(record.get("b"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 32))));
        assertThat(record.get("c"), is((StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 512))));
      }
    }
  }

}
