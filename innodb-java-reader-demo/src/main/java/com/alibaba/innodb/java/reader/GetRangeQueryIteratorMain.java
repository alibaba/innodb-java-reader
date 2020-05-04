/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.util.Utils;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author xu.zx
 */
public class GetRangeQueryIteratorMain {

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

      // ~~~ range query
      Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(
          ImmutableList.of(1), ComparisonOperator.GTE,
          ImmutableList.of(10), ComparisonOperator.LT);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      // ~~~ range query with no upper limit
      iterator = reader.getRangeQueryIterator(
          ImmutableList.of(5), ComparisonOperator.GTE,
          null, ComparisonOperator.NOP);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      // ~~~ range query with no upper limit
      iterator = reader.getRangeQueryIterator(
          ImmutableList.of(6), ComparisonOperator.GTE,
          ImmutableList.of(), ComparisonOperator.NOP);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      // range query with no limit, equivalent to query all
      iterator = reader.getRangeQueryIterator(
          null, ComparisonOperator.NOP,
          null, ComparisonOperator.NOP);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }

      // range query with projection
      iterator = reader.getRangeQueryIterator(
          ImmutableList.of(2), ComparisonOperator.GTE,
          ImmutableList.of(5), ComparisonOperator.LT,
          ImmutableList.of("a"));
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        assert record.getPrimaryKey() == record.get("id");
        System.out.println("id=" + record.get("id"));
        System.out.println("a=" + record.get("a"));
      }
    }
  }

}
