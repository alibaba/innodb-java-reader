/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.fsphdr.FspHdrXes;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.page.inode.Inode;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import java.util.List;

import static com.alibaba.innodb.java.reader.page.PageType.EXTENT_DESCRIPTOR;
import static com.alibaba.innodb.java.reader.page.PageType.FILE_SPACE_HEADER;
import static com.alibaba.innodb.java.reader.page.PageType.INDEX;
import static com.alibaba.innodb.java.reader.page.PageType.INODE;

/**
 * @author xu.zx
 */
public class ShowPagesMain {

  public static void main(String[] args) {
    TableDef tableDef = new TableDef()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false));
    String ibdFilePath = "/usr/local/mysql/data/test/t.ibd";
    try (TableReader reader = new TableReader(ibdFilePath, tableDef)) {
      reader.open();
      List<AbstractPage> pages = reader.readAllPages();
      for (AbstractPage page : pages) {
        StringBuilder sb = new StringBuilder();
        sb.append(page.getPageNumber());
        sb.append(",");
        sb.append(page.pageType().name());
        if (FILE_SPACE_HEADER.equals(page.pageType()) || EXTENT_DESCRIPTOR.equals(page.pageType())) {
          sb.append(",numPagesUsed=");
          sb.append(((FspHdrXes) page).getFspHeader().getNumberOfPagesUsed());
          sb.append(",size=");
          sb.append(((FspHdrXes) page).getFspHeader().getSize());
          sb.append(",xdes.size=");
          sb.append(((FspHdrXes) page).getXdesList().size());
        } else if (INODE.equals(page.pageType())) {
          sb.append(",inode.size=");
          sb.append(((Inode) page).getInodeEntryList().size());
        } else if (INDEX.equals(page.pageType()) && !((Index) page).isLeafPage()) {
          sb.append(",level=");
          sb.append(((Index) page).getIndexHeader().getPageLevel());
          sb.append(",numOfRecs=");
          sb.append(((Index) page).getIndexHeader().getNumOfRecs());
        } else {
          //continue;
        }
        System.out.println(sb.toString());
      }
    }
  }

}
