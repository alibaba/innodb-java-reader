/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author xu.zx
 */
public class QueryAllMain {

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

      // ~~~ query all records
      List<GenericRecord> recordList = reader.queryAll();
      for (GenericRecord record : recordList) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
      }

      // ~~~ query all records with filter, works like index condition pushdown
      Predicate<GenericRecord> predicate = r -> (long) (r.get("a")) == 6L;
      List<GenericRecord> recordList2 = reader.queryAll(predicate);

      // ~~~ query all records with projection
      //[1, 2, null]
      //[2, 4, null]
      //[3, 6, null]
      //[4, 8, null]
      //[5, 10, null]
      //[3, 6, null]
      List<GenericRecord> recordList3 = reader.queryAll(ImmutableList.of("a"));
      for (GenericRecord record : recordList3) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
      }

      // ~~~ query all records with filter and projection
      List<GenericRecord> recordList4 = reader.queryAll(predicate, ImmutableList.of("a"));
      for (GenericRecord record : recordList4) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
      }
    }
  }

}
