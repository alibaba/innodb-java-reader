/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author xu.zx
 */
public class RangeQueryByPrimaryKeyMain {

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
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(1, 3);
      for (GenericRecord record : recordList) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      recordList = reader.rangeQueryByPrimaryKey(null, null);
      for (GenericRecord record : recordList) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      recordList = reader.rangeQueryByPrimaryKey(5, null);
      for (GenericRecord record : recordList) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      // You can filter record like below
      Predicate<GenericRecord> predicate = r -> (long) (r.get("a")) == 12L;
      List<GenericRecord> recordList2 = reader.rangeQueryByPrimaryKey(5, 8, predicate);
    }
  }

}
