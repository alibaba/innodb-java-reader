/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author xu.zx
 */
public class RangeQueryByPrimaryKeyMain {

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

      // ~~~ range query by primary key
      List<GenericRecord> recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(1), ComparisonOperator.GTE,
          ImmutableList.of(3), ComparisonOperator.LT);
      for (GenericRecord record : recordList) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      // range query with no limit, equivalent to query all
      recordList = reader.rangeQueryByPrimaryKey(null, ComparisonOperator.NOP,
          null, ComparisonOperator.NOP);
      for (GenericRecord record : recordList) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      // ~~~ range query with no upper limit
      recordList = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GTE,
          ImmutableList.of(), ComparisonOperator.NOP);
      for (GenericRecord record : recordList) {
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      // ~~~ range query with filter
      Predicate<GenericRecord> predicate = r -> (long) (r.get("a")) == 12L;
      List<GenericRecord> recordList2 = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GT,
          ImmutableList.of(8), ComparisonOperator.LT,
          predicate);

      // ~~~ range query with projection
      List<GenericRecord> recordList3 = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GT,
          ImmutableList.of(8), ComparisonOperator.LT,
          ImmutableList.of("a"));

      // ~~~ range query with filter and projection
      List<GenericRecord> recordList4 = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5), ComparisonOperator.GT,
          ImmutableList.of(8), ComparisonOperator.LT,
          predicate, ImmutableList.of("a"));
    }
  }

}
