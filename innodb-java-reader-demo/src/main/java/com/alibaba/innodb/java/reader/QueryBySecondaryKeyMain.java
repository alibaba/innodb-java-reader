/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author xu.zx
 */
public class QueryBySecondaryKeyMain {

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

      // ~~~ query by sk
      Iterator<GenericRecord> iterator = reader.getRecordIteratorBySk("key_a",
          ImmutableList.of(2L), ComparisonOperator.GTE,
          ImmutableList.of(9L), ComparisonOperator.LT);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
      }

      // ~~~ query by sk, point query
      iterator = reader.getRecordIteratorBySk("key_a",
          ImmutableList.of(6L), ComparisonOperator.GTE,
          ImmutableList.of(6L), ComparisonOperator.LTE);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
      }

      // ~~~ query by sk, projection
      iterator = reader.getRecordIteratorBySk("key_a",
          ImmutableList.of(6L), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP, ImmutableList.of("b"));
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
      }

      // ~~~ query by sk, projection and order
      boolean isAsc = false;
      iterator = reader.getRecordIteratorBySk("key_a",
          ImmutableList.of(6L), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP,
          ImmutableList.of("id", "a", "b"), isAsc);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
      }
    }
  }

}
