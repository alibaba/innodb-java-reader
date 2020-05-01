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

      // for upper null works the same as empty array list
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

      // the same as query all
      iterator = reader.getRangeQueryIterator(
          Utils.constructMinRecord(1), ComparisonOperator.NOP,
          Utils.constructMaxRecord(1), ComparisonOperator.NOP);
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
