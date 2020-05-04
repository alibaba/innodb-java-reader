/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import java.util.Arrays;

/**
 * @author xu.zx
 */
public class QueryByPrimaryKeyMain {

  public static void main(String[] args) {
    String createTableSql = "CREATE TABLE `tb11`\n" +
        "(`id` int(11) NOT NULL ,\n" +
        "`a` bigint(20) NOT NULL,\n" +
        "`b` varchar(64) NOT NULL,\n" +
        "PRIMARY KEY (`id`))\n" +
        "ENGINE=InnoDB;";
    String ibdFilePath = "/usr/local/mysql/data/test/t.ibd";
    try (TableReader reader = new TableReaderImpl(ibdFilePath, createTableSql)) {
      reader.open();

      // ~~~ query by primary key
      GenericRecord record = reader.queryByPrimaryKey(ImmutableList.of(4));
      Object[] values = record.getValues();
      System.out.println(Arrays.asList(values));
      assert record.getPrimaryKey() == record.get("id");
      System.out.println("id=" + record.get("id"));
      System.out.println("a=" + record.get("a"));

      // ~~~ query by primary key with projection
      record = reader.queryByPrimaryKey(ImmutableList.of(4), ImmutableList.of("a"));
    }
  }

}
