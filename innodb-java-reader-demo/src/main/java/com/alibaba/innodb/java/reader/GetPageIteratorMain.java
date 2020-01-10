/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.PageType;

import java.util.Iterator;

/**
 * @author xu.zx
 */
public class GetPageIteratorMain {

  public static void main(String[] args) {
    String createTableSql = "CREATE TABLE `tb11`\n" +
        "(`id` int(11) NOT NULL ,\n" +
        "`a` bigint(20) NOT NULL,\n" +
        "`b` varchar(64) NOT NULL,\n" +
        "PRIMARY KEY (`id`))\n" +
        "ENGINE=InnoDB;";
    String ibdFilePath = "/usr/local/mysql/data/test/t.ibd";
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      Iterator<AbstractPage> iterator = reader.getPageIterator();
      int indexPageCount = 0;
      while (iterator.hasNext()) {
        AbstractPage page = iterator.next();
        if (PageType.INDEX.equals(page.pageType())) {
          indexPageCount++;
        }
      }
      System.out.println(indexPageCount);
    }
  }

}
