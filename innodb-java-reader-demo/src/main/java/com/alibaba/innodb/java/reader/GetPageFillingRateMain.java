/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

/**
 * @author xu.zx
 */
public class GetPageFillingRateMain {

  public static void main(String[] args) {
    String createTableSql = "CREATE TABLE `tb11`\n"
        + "(`id` int(11) NOT NULL ,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "`b` varchar(64) NOT NULL,\n"
        + "PRIMARY KEY (`id`),\n"
        + "KEY `key_a` (`a`))\n"
        + "ENGINE=InnoDB;";
    String ibdFilePath = "/usr/local/mysql/data/test/t.ibd";
    try (TableReader reader = new TableReaderImpl(ibdFilePath, createTableSql)) {
      reader.open();
      System.out.println(reader.getIndexPageFillingRate(3));
      System.out.println(reader.getAllIndexPageFillingRate());
    }
  }

}
